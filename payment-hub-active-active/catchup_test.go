package main

import (
	"testing"
	"time"
)

func TestCatchUpGateEmptyPartitionReady(t *testing.T) {
	g := newCatchUpGate(time.Millisecond)
	g.notePartition("payment-lifecycle", 0, 0, 0)
	// Allow idle path: no messages, empty topic.
	time.Sleep(2 * time.Millisecond)
	g.notePartition("payment-lifecycle", 0, 0, 0)
	ready, reason, _, _, _ := g.snapshot()
	if !ready {
		t.Fatalf("empty topic should be ready, got ready=%v reason=%s", ready, reason)
	}
}

func TestCatchUpGateWaitsForOffsets(t *testing.T) {
	g := newCatchUpGate(time.Millisecond)
	g.notePartition("payment-lifecycle", 0, 0, 10)
	ready, _, _, _, _ := g.snapshot()
	if ready {
		t.Fatal("should not be ready before consuming to HWM")
	}
	g.noteOffset("payment-lifecycle", 0, 9, 10)
	time.Sleep(2 * time.Millisecond)
	g.notePartition("payment-lifecycle", 0, 10, 10)
	ready, reason, _, _, _ := g.snapshot()
	if !ready {
		t.Fatalf("want ready after catching HWM, got reason=%s", reason)
	}
}

func TestCatchUpGateInvalidate(t *testing.T) {
	g := newCatchUpGate(time.Millisecond)
	g.notePartition("t", 0, 0, 0)
	time.Sleep(2 * time.Millisecond)
	g.notePartition("t", 0, 0, 0)
	if ready, _, _, _, _ := g.snapshot(); !ready {
		t.Fatal("expected ready before invalidate")
	}
	g.Invalidate("fenced")
	ready, reason, _, parts, _ := g.snapshot()
	if ready || parts != 0 || reason != "fenced" {
		t.Fatalf("after invalidate: ready=%v reason=%s parts=%d", ready, reason, parts)
	}
}
