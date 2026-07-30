package api

import (
	"sync"
	"time"
)

const trafficLogCap = 200

// TrafficEntry is one observed arbitrator API exchange (shown in the hub log window).
type TrafficEntry struct {
	TS      string `json:"ts"`
	Dir     string `json:"dir"` // in|out
	Method  string `json:"method,omitempty"`
	Path    string `json:"path,omitempty"`
	Cluster string `json:"cluster,omitempty"`
	Summary string `json:"summary"`
}

type TrafficLog struct {
	mu   sync.RWMutex
	seq  int64
	ring []TrafficEntry
}

func NewTrafficLog() *TrafficLog {
	return &TrafficLog{ring: make([]TrafficEntry, 0, trafficLogCap)}
}

func (t *TrafficLog) Add(method, path, cluster, summary string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	entry := TrafficEntry{
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
		Dir:     "in",
		Method:  method,
		Path:    path,
		Cluster: cluster,
		Summary: summary,
	}
	t.ring = append(t.ring, entry)
	if len(t.ring) > trafficLogCap {
		t.ring = t.ring[len(t.ring)-trafficLogCap:]
	}
}

func (t *TrafficLog) List() []TrafficEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]TrafficEntry, len(t.ring))
	copy(out, t.ring)
	// newest first for the UI
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
