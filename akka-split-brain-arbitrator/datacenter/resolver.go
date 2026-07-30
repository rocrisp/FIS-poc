package datacenter

import (
	"fmt"
	"sync"
	"time"
)

// Role values for active/standby HA.
const (
	RoleActive      = "active"      // accepts new traffic
	RoleStandby     = "standby"     // healthy, replicating, not accepting new traffic
	RoleUnreachable = "unreachable" // hub cannot reach this managed cluster
)

// Simulation modes for demo failover without breaking real Submariner.
const (
	SimNone        = "none"
	SimPartition   = "partition"   // mesh down; both sites still reachable
	SimUnreachable = "unreachable" // one site unreachable (+ partition) so the other becomes active
)

type DatacenterStatus struct {
	Name              string    `json:"name"`
	Role              string    `json:"role"`
	AcceptTraffic     bool      `json:"acceptTraffic"`
	Reason            string    `json:"reason"`
	PartitionDetected bool      `json:"partitionDetected"`
	Since             time.Time `json:"since,omitempty"`
	ActiveDatacenter  string    `json:"activeDatacenter,omitempty"`
	StandbyDatacenter string    `json:"standbyDatacenter,omitempty"`
	Simulated         bool      `json:"simulated,omitempty"`
}

type Simulation struct {
	Mode      string    `json:"mode"`             // none|partition|unreachable
	Target    string    `json:"target,omitempty"` // cluster name when mode=unreachable
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	Note      string    `json:"note,omitempty"`
}

type Overview struct {
	Sites               []DatacenterStatus `json:"sites"`
	ObservedSubmariner  bool               `json:"observedSubmarinerConnected"`
	ObservedReachable   map[string]bool    `json:"observedReachability"`
	EffectiveSubmariner bool               `json:"effectiveSubmarinerConnected"`
	Simulation          Simulation         `json:"simulation"`
	Priority            []string           `json:"priority"`
}

type Resolver interface {
	Resolve(callerCluster string) DatacenterStatus
	Snapshot() Overview
	GetSimulation() Simulation
	SetSimulation(mode, target string) error
}

type PriorityResolver struct {
	priority            []string
	mu                  sync.RWMutex
	reachability        map[string]bool
	submarinerConnected bool
	partitionSince      time.Time
	sim                 Simulation
}

func NewPriorityResolver(priority []string) *PriorityResolver {
	return &PriorityResolver{
		priority:            priority,
		reachability:        make(map[string]bool),
		submarinerConnected: true,
		sim:                 Simulation{Mode: SimNone},
	}
}

func (r *PriorityResolver) UpdateReachability(cluster string, reachable bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reachability[cluster] = reachable
}

func (r *PriorityResolver) UpdateSubmarinerStatus(connected bool) {
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

func (r *PriorityResolver) GetSimulation() Simulation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sim
}

func (r *PriorityResolver) SetSimulation(mode, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch mode {
	case "", SimNone:
		r.sim = Simulation{Mode: SimNone, UpdatedAt: time.Now().UTC(), Note: "using live ACM/Submariner signals"}
		return nil
	case SimPartition:
		r.sim = Simulation{
			Mode:      SimPartition,
			UpdatedAt: time.Now().UTC(),
			Note:      "Submariner mesh down; both sites reachable; priority active stays",
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
		for _, p := range r.priority {
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
			Note:      fmt.Sprintf("forced %s unreachable; peer should become active", target),
		}
		if r.partitionSince.IsZero() {
			r.partitionSince = time.Now()
		}
		return nil
	default:
		return fmt.Errorf("unsupported simulation mode %q", mode)
	}
}

func (r *PriorityResolver) Snapshot() Overview {
	r.mu.RLock()
	defer r.mu.RUnlock()

	obsReach := make(map[string]bool, len(r.priority))
	for _, name := range r.priority {
		obsReach[name] = r.reachability[name]
	}
	effReach, effSub := r.effectiveLocked()

	sites := make([]DatacenterStatus, 0, len(r.priority))
	for _, name := range r.priority {
		sites = append(sites, r.resolveLocked(name, effReach, effSub))
	}

	return Overview{
		Sites:               sites,
		ObservedSubmariner:  r.submarinerConnected,
		ObservedReachable:   obsReach,
		EffectiveSubmariner: effSub,
		Simulation:          r.sim,
		Priority:            append([]string(nil), r.priority...),
	}
}

func (r *PriorityResolver) Resolve(callerCluster string) DatacenterStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	effReach, effSub := r.effectiveLocked()
	return r.resolveLocked(callerCluster, effReach, effSub)
}

func (r *PriorityResolver) effectiveLocked() (map[string]bool, bool) {
	reach := make(map[string]bool, len(r.priority))
	for _, name := range r.priority {
		reach[name] = r.reachability[name]
	}
	sub := r.submarinerConnected

	switch r.sim.Mode {
	case SimPartition:
		sub = false
		for _, name := range r.priority {
			reach[name] = true
		}
	case SimUnreachable:
		sub = false
		for _, name := range r.priority {
			if name == r.sim.Target {
				reach[name] = false
			} else {
				reach[name] = true
			}
		}
	}
	return reach, sub
}

func (r *PriorityResolver) resolveLocked(callerCluster string, reach map[string]bool, subConnected bool) DatacenterStatus {
	status := DatacenterStatus{Name: callerCluster, Simulated: r.sim.Mode != SimNone && r.sim.Mode != ""}
	active, standby := r.pickSidesLocked(reach, subConnected)
	status.ActiveDatacenter = active
	status.StandbyDatacenter = standby

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

	switch callerCluster {
	case active:
		status.Role = RoleActive
		status.AcceptTraffic = true
		status.Reason = r.reasonForDecisionLocked(callerCluster, active, reach, subConnected)
	case standby:
		status.Role = RoleStandby
		status.AcceptTraffic = false
		status.Reason = r.reasonForDecisionLocked(callerCluster, active, reach, subConnected)
	default:
		status.Role = RoleStandby
		status.AcceptTraffic = false
		status.Reason = "unknown_cluster"
	}
	if status.Simulated && status.Reason != "simulated_unreachable" {
		status.Reason = "simulated_" + status.Reason
	}
	return status
}

func (r *PriorityResolver) pickSidesLocked(reach map[string]bool, subConnected bool) (active, standby string) {
	if subConnected {
		if len(r.priority) >= 1 {
			active = r.priority[0]
		}
		if len(r.priority) >= 2 {
			standby = r.priority[1]
		}
		return active, standby
	}

	var reachable []string
	for _, cluster := range r.priority {
		if reach[cluster] {
			reachable = append(reachable, cluster)
		}
	}
	if len(reachable) >= 1 {
		active = reachable[0]
	}
	if len(reachable) >= 2 {
		standby = reachable[1]
	} else if len(r.priority) >= 2 {
		for _, cluster := range r.priority {
			if cluster != active {
				standby = cluster
				break
			}
		}
	}
	return active, standby
}

func (r *PriorityResolver) reasonForDecisionLocked(caller, active string, reach map[string]bool, subConnected bool) string {
	if !subConnected {
		if caller == active {
			if allReachable(r.priority, reach) {
				return "priority"
			}
			return "reachable"
		}
		return "standby_during_partition"
	}
	if caller == active {
		return "priority"
	}
	return "standby"
}

func allReachable(priority []string, reach map[string]bool) bool {
	for _, cluster := range priority {
		if !reach[cluster] {
			return false
		}
	}
	return true
}
