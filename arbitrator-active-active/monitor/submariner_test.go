package monitor

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockUpdater struct {
	reachability        map[string]bool
	submarinerConnected bool
}

func newMockUpdater() *mockUpdater {
	return &mockUpdater{reachability: make(map[string]bool)}
}

func (m *mockUpdater) UpdateReachability(cluster string, reachable bool) {
	m.reachability[cluster] = reachable
}

func (m *mockUpdater) UpdateSubmarinerStatus(connected bool) {
	m.submarinerConnected = connected
}

func TestInitialState(t *testing.T) {
	updater := newMockUpdater()
	mon := NewSubmarinerMonitor(updater, 100*time.Millisecond, 200*time.Millisecond)

	if mon.IsPartitioned() {
		t.Fatal("expected not partitioned initially")
	}
	conns := mon.GetConnections()
	if len(conns) != 0 {
		t.Fatalf("expected no connections, got %d", len(conns))
	}
}

func TestDetectsPartition(t *testing.T) {
	updater := newMockUpdater()
	mon := NewSubmarinerMonitor(updater, 50*time.Millisecond, 150*time.Millisecond)

	// Simulate: both reachable, Submariner down
	mon.SetGatewayFetcher(func() (map[string]bool, bool, error) {
		return map[string]bool{"cluster1-fis": true, "cluster2-fis": true}, false, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mon.Start(ctx)

	// Wait for stabilization (150ms) + a poll cycle
	time.Sleep(300 * time.Millisecond)

	if !mon.IsPartitioned() {
		t.Fatal("expected partition after stabilization")
	}
	if updater.submarinerConnected {
		t.Fatal("expected updater to show Submariner disconnected")
	}
}

func TestRecovery(t *testing.T) {
	updater := newMockUpdater()
	mon := NewSubmarinerMonitor(updater, 50*time.Millisecond, 100*time.Millisecond)

	var mu sync.RWMutex
	connected := true
	mon.SetGatewayFetcher(func() (map[string]bool, bool, error) {
		mu.RLock()
		defer mu.RUnlock()
		return map[string]bool{"cluster1-fis": true, "cluster2-fis": true}, connected, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	go mon.Start(ctx)

	// Start disconnected — wait past poll + stabilization (100ms).
	mu.Lock()
	connected = false
	mu.Unlock()
	time.Sleep(350 * time.Millisecond)
	if !mon.IsPartitioned() {
		t.Fatal("expected partition")
	}

	// Reconnect — recovery is immediate once connected is observed.
	mu.Lock()
	connected = true
	mu.Unlock()
	time.Sleep(350 * time.Millisecond)
	if mon.IsPartitioned() {
		t.Fatal("expected recovery after stabilization")
	}
}
