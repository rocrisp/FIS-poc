package monitor

import (
	"context"
	"log"
	"sync"
	"time"
)

type ReachabilityUpdater interface {
	UpdateReachability(cluster string, reachable bool)
	UpdateSubmarinerStatus(connected bool)
}

type ConnectionStatus struct {
	Source         string
	Target         string
	Connected      bool
	LastTransition time.Time
	Message        string
}

type SubmarinerMonitor struct {
	updater        ReachabilityUpdater
	pollInterval   time.Duration
	stabilization  time.Duration
	fetcher        func() (map[string]bool, bool, error)

	mu             sync.RWMutex
	partitioned    bool
	connections    map[string]ConnectionStatus
	pendingState   *bool
	stateChangedAt time.Time
}

func NewSubmarinerMonitor(updater ReachabilityUpdater, pollInterval, stabilization time.Duration) *SubmarinerMonitor {
	return &SubmarinerMonitor{
		updater:       updater,
		pollInterval:  pollInterval,
		stabilization: stabilization,
		connections:   make(map[string]ConnectionStatus),
		fetcher: func() (map[string]bool, bool, error) {
			return map[string]bool{}, true, nil
		},
	}
}

func (m *SubmarinerMonitor) SetGatewayFetcher(fn func() (map[string]bool, bool, error)) {
	m.fetcher = fn
}

func (m *SubmarinerMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.poll()
		}
	}
}

func (m *SubmarinerMonitor) poll() {
	reachability, submarinerConnected, err := m.fetcher()
	if err != nil {
		log.Printf("submariner monitor: fetch error: %v", err)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Update reachability on resolver
	for cluster, reachable := range reachability {
		m.updater.UpdateReachability(cluster, reachable)
	}

	// Apply stabilization
	currentPartitioned := !submarinerConnected
	if m.pendingState == nil || *m.pendingState != currentPartitioned {
		m.pendingState = &currentPartitioned
		m.stateChangedAt = time.Now()
		return
	}

	if time.Since(m.stateChangedAt) < m.stabilization {
		return
	}

	// State is stable — apply it
	if m.partitioned != currentPartitioned {
		m.partitioned = currentPartitioned
		m.updater.UpdateSubmarinerStatus(submarinerConnected)
		if currentPartitioned {
			log.Printf("submariner monitor: partition detected")
		} else {
			log.Printf("submariner monitor: connectivity restored")
		}
	}
}

func (m *SubmarinerMonitor) IsPartitioned() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.partitioned
}

func (m *SubmarinerMonitor) GetConnections() map[string]ConnectionStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]ConnectionStatus, len(m.connections))
	for k, v := range m.connections {
		result[k] = v
	}
	return result
}
