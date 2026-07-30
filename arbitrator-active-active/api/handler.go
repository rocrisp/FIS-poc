package api

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fis/arbitrator-active-active/datacenter"
	"github.com/fis/arbitrator-active-active/state"
)

type Handler struct {
	store    *state.Store
	resolver datacenter.Resolver
	started  time.Time
	ui       fs.FS
	traffic  *TrafficLog
}

func NewHandler(store *state.Store, resolver datacenter.Resolver) *Handler {
	return &Handler{
		store:    store,
		resolver: resolver,
		started:  time.Now(),
		traffic:  NewTrafficLog(),
	}
}

// SetUI attaches the embedded hub dashboard filesystem (web/).
func (h *Handler) SetUI(ui fs.FS) {
	h.ui = ui
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/state", h.getState)
	mux.HandleFunc("PUT /api/v1/state", h.putState)
	mux.HandleFunc("GET /api/v1/health", h.getHealth)
	mux.HandleFunc("GET /api/v1/overview", h.getOverview)
	mux.HandleFunc("GET /api/v1/simulation", h.getSimulation)
	mux.HandleFunc("PUT /api/v1/simulation", h.putSimulation)
	mux.HandleFunc("POST /api/v1/simulation", h.putSimulation)
	mux.HandleFunc("GET /api/v1/traffic", h.getTraffic)

	if h.ui != nil {
		mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(h.ui))))
		mux.HandleFunc("GET /{$}", h.serveUI)
		mux.HandleFunc("GET /ui", h.serveUI)
	}
}

type stateResponse struct {
	Version      int64                       `json:"version"`
	Data         json.RawMessage             `json:"data"`
	LastModified string                      `json:"lastModified"`
	Datacenter   datacenter.DatacenterStatus `json:"datacenter"`
	Partition    partitionInfo               `json:"partition"`
	Simulation   datacenter.Simulation       `json:"simulation"`
}

type partitionInfo struct {
	Detected         bool     `json:"detected"`
	Since            string   `json:"since,omitempty"`
	SubmarinerStatus string   `json:"submarinerStatus,omitempty"`
	ActivePeers      []string `json:"activePeers,omitempty"`
	SoleActiveSite   string   `json:"soleActiveSite,omitempty"`
	WriteMode        string   `json:"writeMode,omitempty"`
	FallbackActive   string   `json:"fallbackActive,omitempty"`
}

type putRequest struct {
	Data json.RawMessage `json:"data"`
}

type simulationRequest struct {
	Mode   string `json:"mode"`
	Target string `json:"target"`
}

func (h *Handler) serveUI(w http.ResponseWriter, _ *http.Request) {
	b, err := fs.ReadFile(h.ui, "index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func (h *Handler) getTraffic(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": h.traffic.List()})
}

func (h *Handler) getState(w http.ResponseWriter, r *http.Request) {
	clusterName := r.Header.Get("X-Cluster-Name")
	data, version, updated := h.store.Read()
	dcStatus := h.resolver.Resolve(clusterName)

	resp := h.buildResponse(data, version, updated, dcStatus)
	h.traffic.Add("GET", "/api/v1/state", clusterName, fmt.Sprintf(
		"→ role=%s acceptTraffic=%v reason=%s partition=%v sim=%s",
		dcStatus.Role, dcStatus.AcceptTraffic, dcStatus.Reason, dcStatus.PartitionDetected, resp.Simulation.Mode,
	))
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, version))
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) putState(w http.ResponseWriter, r *http.Request) {
	clusterName := r.Header.Get("X-Cluster-Name")

	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		h.traffic.Add("PUT", "/api/v1/state", clusterName, "→ 400 missing_if_match")
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "missing_if_match",
			"message": "If-Match header is required for optimistic locking",
		})
		return
	}

	expectedVersion, err := parseETag(ifMatch)
	if err != nil {
		h.traffic.Add("PUT", "/api/v1/state", clusterName, "→ 400 invalid_etag")
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_etag",
			"message": "If-Match header must be a quoted integer",
		})
		return
	}

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.traffic.Add("PUT", "/api/v1/state", clusterName, "→ 400 invalid_body")
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_body",
			"message": err.Error(),
		})
		return
	}

	newVersion, err := h.store.Write(req.Data, expectedVersion)
	if err == state.ErrVersionConflict {
		h.traffic.Add("PUT", "/api/v1/state", clusterName, fmt.Sprintf("→ 409 version_conflict current=%d", newVersion))
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, newVersion))
		writeJSON(w, http.StatusConflict, map[string]interface{}{
			"error":          "version_conflict",
			"message":        "State was modified by another node. Read the current state and retry.",
			"currentVersion": newVersion,
		})
		return
	}
	if err != nil {
		log.Printf("state write error: %v", err)
		h.traffic.Add("PUT", "/api/v1/state", clusterName, "→ 500 "+err.Error())
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	data, version, updated := h.store.Read()
	dcStatus := h.resolver.Resolve(clusterName)
	resp := h.buildResponse(data, version, updated, dcStatus)
	h.traffic.Add("PUT", "/api/v1/state", clusterName, fmt.Sprintf("→ ok version=%d role=%s", version, dcStatus.Role))
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, version))
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) getHealth(w http.ResponseWriter, _ *http.Request) {
	uptime := time.Since(h.started).Round(time.Second).String()
	sim := h.resolver.GetSimulation()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":               "ok",
		"submarinerMonitoring": "active",
		"uptime":               uptime,
		"simulationMode":       sim.Mode,
	})
}

func (h *Handler) getOverview(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.resolver.Snapshot())
}

func (h *Handler) getSimulation(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.resolver.GetSimulation())
}

func (h *Handler) putSimulation(w http.ResponseWriter, r *http.Request) {
	var req simulationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.traffic.Add(r.Method, "/api/v1/simulation", "", "→ 400 invalid_body")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_body", "message": err.Error()})
		return
	}
	if err := h.resolver.SetSimulation(req.Mode, req.Target); err != nil {
		h.traffic.Add(r.Method, "/api/v1/simulation", req.Target, "→ 400 "+err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_simulation", "message": err.Error()})
		return
	}
	log.Printf("simulation set mode=%s target=%s", req.Mode, req.Target)
	h.traffic.Add(r.Method, "/api/v1/simulation", req.Target, fmt.Sprintf("→ set mode=%s target=%s", req.Mode, req.Target))
	writeJSON(w, http.StatusOK, h.resolver.Snapshot())
}

func (h *Handler) buildResponse(data json.RawMessage, version int64, updated time.Time, dc datacenter.DatacenterStatus) stateResponse {
	if data == nil {
		data = json.RawMessage(`{}`)
	}

	resp := stateResponse{
		Version:    version,
		Data:       data,
		Datacenter: dc,
		Simulation: h.resolver.GetSimulation(),
	}
	if !updated.IsZero() {
		resp.LastModified = updated.UTC().Format(time.RFC3339)
	}

	ov := h.resolver.Snapshot()
	resp.Partition = partitionInfo{
		Detected:       dc.PartitionDetected,
		ActivePeers:    dc.ActivePeers,
		SoleActiveSite: ov.SoleActiveSite,
		WriteMode:      ov.WriteMode,
		FallbackActive: ov.FallbackActive,
	}
	if dc.PartitionDetected {
		resp.Partition.SubmarinerStatus = "disconnected"
		if !dc.Since.IsZero() {
			resp.Partition.Since = dc.Since.UTC().Format(time.RFC3339)
		}
	}

	return resp
}

func parseETag(etag string) (int64, error) {
	trimmed := strings.Trim(etag, `"`)
	return strconv.ParseInt(trimmed, 10, 64)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("failed to encode JSON response: %v", err)
	}
}
