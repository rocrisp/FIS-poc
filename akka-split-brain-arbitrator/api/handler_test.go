package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fis/akka-split-brain-arbitrator/datacenter"
	"github.com/fis/akka-split-brain-arbitrator/state"
)

type mockResolver struct {
	status datacenter.DatacenterStatus
}

func (m *mockResolver) Resolve(callerCluster string) datacenter.DatacenterStatus {
	s := m.status
	s.Name = callerCluster
	return s
}

func (m *mockResolver) Snapshot() datacenter.Overview {
	return datacenter.Overview{Sites: []datacenter.DatacenterStatus{m.status}}
}

func (m *mockResolver) GetSimulation() datacenter.Simulation {
	return datacenter.Simulation{Mode: datacenter.SimNone}
}

func (m *mockResolver) SetSimulation(mode, target string) error { return nil }

func TestGetStateEmpty(t *testing.T) {
	store := state.NewStore()
	resolver := &mockResolver{status: datacenter.DatacenterStatus{Role: "active", AcceptTraffic: true, Reason: "priority"}}
	h := NewHandler(store, resolver)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/state", nil)
	req.Header.Set("X-Cluster-Name", "cluster1-fis")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("ETag") != `"0"` {
		t.Fatalf("expected ETag \"0\", got %s", w.Header().Get("ETag"))
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["version"].(float64) != 0 {
		t.Fatalf("expected version 0, got %v", resp["version"])
	}
	dc := resp["datacenter"].(map[string]interface{})
	if dc["role"] != "active" {
		t.Fatalf("expected active, got %v", dc["role"])
	}
}

func TestPutStateSuccess(t *testing.T) {
	store := state.NewStore()
	resolver := &mockResolver{status: datacenter.DatacenterStatus{Role: "active", AcceptTraffic: true, Reason: "priority"}}
	h := NewHandler(store, resolver)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"data":{"decision":"keep-cluster1"}}`
	req := httptest.NewRequest("PUT", "/api/v1/state", bytes.NewBufferString(body))
	req.Header.Set("If-Match", `"0"`)
	req.Header.Set("X-Cluster-Name", "cluster1-fis")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("ETag") != `"1"` {
		t.Fatalf("expected ETag \"1\", got %s", w.Header().Get("ETag"))
	}
}

func TestPutStateConflict(t *testing.T) {
	store := state.NewStore()
	resolver := &mockResolver{status: datacenter.DatacenterStatus{Role: "active", AcceptTraffic: true, Reason: "priority"}}
	h := NewHandler(store, resolver)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// First write succeeds
	body := `{"data":{"decision":"keep-cluster1"}}`
	req := httptest.NewRequest("PUT", "/api/v1/state", bytes.NewBufferString(body))
	req.Header.Set("If-Match", `"0"`)
	req.Header.Set("X-Cluster-Name", "cluster1-fis")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Second write with stale version
	body2 := `{"data":{"decision":"keep-cluster2"}}`
	req2 := httptest.NewRequest("PUT", "/api/v1/state", bytes.NewBufferString(body2))
	req2.Header.Set("If-Match", `"0"`)
	req2.Header.Set("X-Cluster-Name", "cluster2-fis")
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w2.Code, w2.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["error"] != "version_conflict" {
		t.Fatalf("expected version_conflict error, got %v", resp["error"])
	}
}

func TestPutStateMissingIfMatch(t *testing.T) {
	store := state.NewStore()
	resolver := &mockResolver{status: datacenter.DatacenterStatus{Role: "active", AcceptTraffic: true, Reason: "priority"}}
	h := NewHandler(store, resolver)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"data":{"decision":"keep-cluster1"}}`
	req := httptest.NewRequest("PUT", "/api/v1/state", bytes.NewBufferString(body))
	req.Header.Set("X-Cluster-Name", "cluster1-fis")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestGetHealth(t *testing.T) {
	store := state.NewStore()
	resolver := &mockResolver{status: datacenter.DatacenterStatus{Role: "active", AcceptTraffic: true}}
	h := NewHandler(store, resolver)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("expected ok, got %v", resp["status"])
	}
}
