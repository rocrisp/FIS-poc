package main

import (
	"fmt"
	"sync"
	"time"
)

// catchUpGate blocks new payments until this site has consumed Kafka up to
// the high-watermark for every assigned ledger partition. Re-arm (Invalidate)
// when the site is fenced so a stale ledger cannot accept I/O after return.
type catchUpGate struct {
	mu        sync.RWMutex
	ready     bool
	reason    string
	gen       uint64
	parts     map[string]*partProg
	lastMsgAt time.Time
	idleAfter time.Duration
}

type partProg struct {
	target int64 // high watermark when last observed
	pos    int64 // last consumed offset; -1 if none yet
	done   bool
}

func newCatchUpGate(idleAfter time.Duration) *catchUpGate {
	if idleAfter <= 0 {
		idleAfter = 2 * time.Second
	}
	return &catchUpGate{
		reason:    "catching_up_starting",
		parts:     make(map[string]*partProg),
		idleAfter: idleAfter,
	}
}

func (g *catchUpGate) snapshot() (ready bool, reason string, gen uint64, parts int, done int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, p := range g.parts {
		parts++
		if p.done {
			done++
		}
	}
	return g.ready, g.reason, g.gen, parts, done
}

// Invalidate forces another catch-up cycle (startup, or site fenced).
func (g *catchUpGate) Invalidate(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ready = false
	if reason == "" {
		reason = "catching_up"
	}
	g.reason = reason
	g.gen++
	g.parts = make(map[string]*partProg)
	g.lastMsgAt = time.Time{}
}

// notePartition refreshes progress for an assigned claim (call on start + ticker).
func (g *catchUpGate) notePartition(topic string, partition int32, initialOffset, hwm int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	p, ok := g.parts[key]
	if !ok {
		p = &partProg{pos: initialOffset - 1, target: hwm}
		g.parts[key] = p
	}
	if hwm > p.target {
		p.target = hwm
		p.done = false
	}
	// Empty / already at tip.
	if initialOffset >= hwm && p.pos < initialOffset-1 {
		p.pos = hwm - 1
	}
	if p.pos+1 >= p.target {
		p.done = true
	}
	g.evalLocked()
}

func (g *catchUpGate) noteOffset(topic string, partition int32, offset, hwm int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	p, ok := g.parts[key]
	if !ok {
		p = &partProg{pos: offset, target: hwm}
		g.parts[key] = p
	} else {
		if offset > p.pos {
			p.pos = offset
		}
		if hwm > p.target {
			p.target = hwm
			p.done = false
		}
	}
	if p.pos+1 >= p.target {
		p.done = true
	}
	g.lastMsgAt = time.Now()
	g.evalLocked()
}

func (g *catchUpGate) evalLocked() {
	if g.ready {
		return
	}
	if len(g.parts) == 0 {
		g.reason = "catching_up_waiting_assignment"
		return
	}
	allDone := true
	done := 0
	for _, p := range g.parts {
		if p.done {
			done++
		} else {
			allDone = false
		}
	}
	if !allDone {
		g.reason = fmt.Sprintf("catching_up_%d/%d_partitions", done, len(g.parts))
		return
	}
	// Brief idle so we don't open mid-burst while HWM is still moving.
	if !g.lastMsgAt.IsZero() && time.Since(g.lastMsgAt) < g.idleAfter {
		g.reason = "catching_up_draining"
		return
	}
	g.ready = true
	g.reason = "ledger_ready"
}
