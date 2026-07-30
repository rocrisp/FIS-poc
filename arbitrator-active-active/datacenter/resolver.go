package datacenter

import (
	"fmt"
	"sync"
	"time"
)

// Role values for active-active HA.
const (
	RoleActive      = "active"      // accepts traffic (both sites when healthy)
	RoleUnreachable = "unreachable" // hub cannot reach this managed cluster — fenced
)

// Simulation modes for demo without breaking real Submariner.
const (
	SimNone        = "none"
	SimPartition   = "partition"   // mesh down; both sites still active if reachable
	SimUnreachable = "unreachable" // one site fenced; peer stays active
)

type DatacenterStatus struct {
	Name              string    `json:"name"`
	Role              string    `json:"role"`
	AcceptTraffic     bool      `json:"acceptTraffic"`
	Reason            string    `json:"reason"`
	PartitionDetected bool      `json:"partitionDetected"`
	Since             time.Time `json:"since,omitempty"`
	Mode              string    `json:"mode"` // active-active
	ActivePeers       []string  `json:"activePeers,omitempty"`
	Simulated         bool      `json:"simulated,omitempty"`
}

type Simulation struct {
	Mode      string    `json:"mode"`
	Target    string    `json:"target,omitempty"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	Note      string    `json:"note,omitempty"`
}

type Overview struct {
	Mode                string             `json:"mode"`
	Sites               []DatacenterStatus `json:"sites"`
	ObservedSubmariner  bool               `json:"observedSubmarinerConnected"`
	ObservedReachable   map[string]bool    `json:"observedReachability"`
	EffectiveSubmariner bool               `json:"effectiveSubmarinerConnected"`
	Simulation          Simulation         `json:"simulation"`
	Priority            []string           `json:"priority"` // site list (not standby order)
}

type Resolver interface {
	Resolve(callerCluster string) DatacenterStatus
	Snapshot() Overview
	GetSimulation() Simulation
	SetSimulation(mode, target string) error
}

type ActiveActiveResolver struct {
	sites               []string
	mu                  sync.RWMutex
	reachability        map[string]bool
	submarinerConnected bool
	partitionSince      time.Time
	sim                 Simulation
}

func NewActiveActiveResolver(sites []string) *ActiveActiveResolver {
	return &ActiveActiveResolver{
		sites:               sites,
		reachability:        make(map[string]bool),
		submarinerConnected: true,
		sim:                 Simulation{Mode: SimNone},
	}
}

// NewPriorityResolver kept as alias so main.go wiring stays familiar.
func NewPriorityResolver(sites []string) *ActiveActiveResolver {
	return NewActiveActiveResolver(sites)
}

func (r *ActiveActiveResolver) UpdateReachability(cluster string, reachable bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reachability[cluster] = reachable
}

func (r *ActiveActiveResolver) UpdateSubmarinerStatus(connected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wasConnected := r.submarinerConnected
	r.submarinerConnected = connected
	if wasConnected && !connected {
		r.partitionSince = time.Now()
	}
	if connected {
		r.partitionSince = time.Time{}
	}
}

func (r *ActiveActiveResolver) GetSimulation() Simulation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sim
}

func (r *ActiveActiveResolver) SetSimulation(mode, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch mode {
	case "", SimNone:
		r.sim = Simulation{Mode: SimNone, UpdatedAt: time.Now().UTC(), Note: "using live ACM/Submariner signals; both reachable sites active"}
		return nil
	case SimPartition:
		r.sim = Simulation{
			Mode:      SimPartition,
			UpdatedAt: time.Now().UTC(),
			Note:      "Submariner mesh down; both reachable sites still accept traffic (sync may lag)",
		}
		if r.partitionSince.IsZero() {
			r.partitionSince = time.Now()
		}
		return nil
	case SimUnreachable:
		if target == "" {
			return fmt.Errorf("target cluster required for mode=%s", SimUnreachable)
		}
		known := false
		for _, p := range r.sites {
			if p == target {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("unknown target cluster %q", target)
		}
		r.sim = Simulation{
			Mode:      SimUnreachable,
			Target:    target,
			UpdatedAt: time.Now().UTC(),
			Note:      fmt.Sprintf("forced %s unreachable/fenced; peer stays active", target),
		}
		if r.partitionSince.IsZero() {
			r.partitionSince = time.Now()
		}
		return nil
	default:
		return fmt.Errorf("unsupported simulation mode %q", mode)
	}
}

func (r *ActiveActiveResolver) Snapshot() Overview {
	r.mu.RLock()
	defer r.mu.RUnlock()

	obsReach := make(map[string]bool, len(r.sites))
	for _, name := range r.sites {
		obsReach[name] = r.reachability[name]
	}
	effReach, effSub := r.effectiveLocked()

	sites := make([]DatacenterStatus, 0, len(r.sites))
	for _, name := range r.sites {
		sites = append(sites, r.resolveLocked(name, effReach, effSub))
	}

	return Overview{
		Mode:                "active-active",
		Sites:               sites,
		ObservedSubmariner:  r.submarinerConnected,
		ObservedReachable:   obsReach,
		EffectiveSubmariner: effSub,
		Simulation:          r.sim,
		Priority:            append([]string(nil), r.sites...),
	}
}

func (r *ActiveActiveResolver) Resolve(callerCluster string) DatacenterStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	effReach, effSub := r.effectiveLocked()
	return r.resolveLocked(callerCluster, effReach, effSub)
}

func (r *ActiveActiveResolver) effectiveLocked() (map[string]bool, bool) {
	reach := make(map[string]bool, len(r.sites))
	for _, name := range r.sites {
		reach[name] = r.reachability[name]
	}
	sub := r.submarinerConnected

	switch r.sim.Mode {
	case SimPartition:
		sub = false
		for _, name := range r.sites {
			reach[name] = true
		}
	case SimUnreachable:
		sub = false
		for _, name := range r.sites {
			if name == r.sim.Target {
				reach[name] = false
			} else {
				reach[name] = true
			}
		}
	}
	return reach, sub
}

func (r *ActiveActiveResolver) activePeersLocked(reach map[string]bool) []string {
	var peers []string
	for _, name := range r.sites {
		if reach[name] {
			peers = append(peers, name)
		}
	}
	return peers
}

func (r *ActiveActiveResolver) resolveLocked(callerCluster string, reach map[string]bool, subConnected bool) DatacenterStatus {
	status := DatacenterStatus{
		Name:      callerCluster,
		Mode:      "active-active",
		Simulated: r.sim.Mode != SimNone && r.sim.Mode != "",
	}
	peers := r.activePeersLocked(reach)
	status.ActivePeers = peers

	if !subConnected {
		status.PartitionDetected = true
		status.Since = r.partitionSince
	}

	if reachable, known := reach[callerCluster]; known && !reachable {
		status.Role = RoleUnreachable
		status.AcceptTraffic = false
		status.Reason = "unreachable"
		if status.Simulated {
			status.Reason = "simulated_unreachable"
		}
		return status
	}

	// Unknown cluster name: do not accept.
	known := false
	for _, s := range r.sites {
		if s == callerCluster {
			known = true
			break
		}
	}
	if !known {
		status.Role = RoleUnreachable
		status.AcceptTraffic = false
		status.Reason = "unknown_cluster"
		return status
	}

	// Active-active: every reachable site accepts traffic.
	status.Role = RoleActive
	status.AcceptTraffic = true
	if !subConnected {
		status.Reason = "active_mesh_degraded"
	} else {
		status.Reason = "active_peer"
	}
	if status.Simulated {
		status.Reason = "simulated_" + status.Reason
	}
	return status
}
