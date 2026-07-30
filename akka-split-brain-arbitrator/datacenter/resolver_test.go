package datacenter

import (
	"testing"
)

func TestHealthyActiveStandby(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(true)

	active := r.Resolve("cluster1-fis")
	if active.PartitionDetected {
		t.Fatal("expected no partition")
	}
	if active.Role != RoleActive || !active.AcceptTraffic {
		t.Fatalf("expected active accepting traffic, got role=%s accept=%v", active.Role, active.AcceptTraffic)
	}
	if active.ActiveDatacenter != "cluster1-fis" || active.StandbyDatacenter != "cluster2-fis" {
		t.Fatalf("unexpected sides: active=%s standby=%s", active.ActiveDatacenter, active.StandbyDatacenter)
	}

	standby := r.Resolve("cluster2-fis")
	if standby.Role != RoleStandby || standby.AcceptTraffic {
		t.Fatalf("expected standby not accepting traffic, got role=%s accept=%v", standby.Role, standby.AcceptTraffic)
	}
	if standby.Reason != "standby" {
		t.Fatalf("expected reason standby, got %s", standby.Reason)
	}
}

func TestOneUnreachableDuringPartition(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", false)
	r.UpdateSubmarinerStatus(false)

	active := r.Resolve("cluster1-fis")
	if !active.PartitionDetected {
		t.Fatal("expected partition")
	}
	if active.Role != RoleActive || !active.AcceptTraffic {
		t.Fatalf("expected active, got role=%s accept=%v", active.Role, active.AcceptTraffic)
	}
	if active.ActiveDatacenter != "cluster1-fis" {
		t.Fatalf("expected cluster1-fis as active, got %s", active.ActiveDatacenter)
	}

	standby := r.Resolve("cluster2-fis")
	if standby.Role != RoleUnreachable || standby.AcceptTraffic {
		t.Fatalf("expected unreachable, got role=%s accept=%v", standby.Role, standby.AcceptTraffic)
	}
}

func TestPriorityTiebreakDuringPartition(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(false)

	active := r.Resolve("cluster1-fis")
	if !active.PartitionDetected {
		t.Fatal("expected partition")
	}
	if active.Role != RoleActive {
		t.Fatalf("expected active (priority winner), got %s", active.Role)
	}
	if active.Reason != "priority" {
		t.Fatalf("expected reason priority, got %s", active.Reason)
	}

	standby := r.Resolve("cluster2-fis")
	if standby.Role != RoleStandby || standby.AcceptTraffic {
		t.Fatalf("expected standby during partition, got role=%s accept=%v", standby.Role, standby.AcceptTraffic)
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
	if status.Role != RoleStandby || status.AcceptTraffic {
		t.Fatalf("expected standby without traffic, got role=%s accept=%v", status.Role, status.AcceptTraffic)
	}
}
