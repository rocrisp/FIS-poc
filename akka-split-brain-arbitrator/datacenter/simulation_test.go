package datacenter

import "testing"

func TestSimulationUnreachablePromotesPeer(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(true)

	if err := r.SetSimulation(SimUnreachable, "cluster1-fis"); err != nil {
		t.Fatal(err)
	}

	c1 := r.Resolve("cluster1-fis")
	if c1.Role != RoleUnreachable || c1.AcceptTraffic {
		t.Fatalf("cluster1: want unreachable, got role=%s accept=%v", c1.Role, c1.AcceptTraffic)
	}
	c2 := r.Resolve("cluster2-fis")
	if c2.Role != RoleActive || !c2.AcceptTraffic {
		t.Fatalf("cluster2: want active, got role=%s accept=%v", c2.Role, c2.AcceptTraffic)
	}
	if !c2.PartitionDetected || !c2.Simulated {
		t.Fatalf("expected simulated partition on peer")
	}

	if err := r.SetSimulation(SimNone, ""); err != nil {
		t.Fatal(err)
	}
	c1 = r.Resolve("cluster1-fis")
	if c1.Role != RoleActive || !c1.AcceptTraffic {
		t.Fatalf("after clear: cluster1 should be active again, got %s", c1.Role)
	}
}

func TestSimulationPartitionKeepsPriorityActive(t *testing.T) {
	r := NewPriorityResolver([]string{"cluster1-fis", "cluster2-fis"})
	r.UpdateReachability("cluster1-fis", true)
	r.UpdateReachability("cluster2-fis", true)
	r.UpdateSubmarinerStatus(true)

	if err := r.SetSimulation(SimPartition, ""); err != nil {
		t.Fatal(err)
	}
	c1 := r.Resolve("cluster1-fis")
	c2 := r.Resolve("cluster2-fis")
	if c1.Role != RoleActive || !c1.AcceptTraffic || !c1.PartitionDetected {
		t.Fatalf("unexpected c1: %+v", c1)
	}
	if c2.Role != RoleStandby || c2.AcceptTraffic {
		t.Fatalf("unexpected c2: %+v", c2)
	}
}
