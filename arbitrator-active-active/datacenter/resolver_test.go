package datacenter

import (
	"testing"
)

func TestHealthyBothActive(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(true)

	c1 := r.Resolve("cluster1-fis")
	if c1.PartitionDetected {
		t.Fatal("expected no partition")
	}
	if c1.Role != RoleActive || !c1.AcceptTraffic {
		t.Fatalf("expected active accepting traffic, got role=%s accept=%v", c1.Role, c1.AcceptTraffic)
	}
	if c1.Reason != "active_peer" {
		t.Fatalf("expected reason active_peer, got %s", c1.Reason)
	}
	if len(c1.ActivePeers) != 2 {
		t.Fatalf("expected 2 active peers, got %v", c1.ActivePeers)
	}

	c2 := r.Resolve("cluster2-fis")
	if c2.Role != RoleActive || !c2.AcceptTraffic {
		t.Fatalf("expected peer also active, got role=%s accept=%v", c2.Role, c2.AcceptTraffic)
	}
	if c2.Mode != "active-active" {
		t.Fatalf("expected mode active-active, got %s", c2.Mode)
	}
}

func TestOneUnreachableFencesSite(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", false)
	r.UpdateSubmarinerStatus(false)

	c1 := r.Resolve("cluster1-fis")
	if !c1.PartitionDetected {
		t.Fatal("expected partition")
	}
	if c1.Role != RoleActive || !c1.AcceptTraffic {
		t.Fatalf("expected active, got role=%s accept=%v", c1.Role, c1.AcceptTraffic)
	}
	if len(c1.ActivePeers) != 1 || c1.ActivePeers[0] != "cluster1-fis" {
		t.Fatalf("expected only cluster1 active, got %v", c1.ActivePeers)
	}

	c2 := r.Resolve("cluster2-fis")
	if c2.Role != RoleUnreachable || c2.AcceptTraffic {
		t.Fatalf("expected unreachable, got role=%s accept=%v", c2.Role, c2.AcceptTraffic)
	}
}

func TestMeshDownBothStillActive(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(false)

	c1 := r.Resolve("cluster1-fis")
	c2 := r.Resolve("cluster2-fis")
	if !c1.PartitionDetected || !c2.PartitionDetected {
		t.Fatal("expected partition on both")
	}
	if c1.Role != RoleActive || !c1.AcceptTraffic || c1.Reason != "active_mesh_degraded" {
		t.Fatalf("unexpected c1: %+v", c1)
	}
	if c2.Role != RoleActive || !c2.AcceptTraffic || c2.Reason != "active_mesh_degraded" {
		t.Fatalf("unexpected c2: %+v", c2)
	}
}

func TestUnknownCaller(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(true)

	status := r.Resolve("unknown-cluster")
	if status.Name != "unknown-cluster" {
		t.Fatalf("expected unknown-cluster, got %s", status.Name)
	}
	if status.Role != RoleUnreachable || status.AcceptTraffic {
		t.Fatalf("expected unreachable without traffic, got role=%s accept=%v", status.Role, status.AcceptTraffic)
	}
}
