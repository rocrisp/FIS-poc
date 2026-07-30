// Demo test console: runs live checks against the FIS dual-site stack
// and serves a small UI that shows pass/fail for each case.
package main

import (
	"bytes"
	"crypto/tls"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed web/*
var webFS embed.FS

type endpoints struct {
	Arbitrator string `json:"arbitrator"`
	ActiveHub  string `json:"activeHub"`
	StandbyHub string `json:"standbyHub"`
}

type testCase struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Group       string `json:"group"`
	Description string `json:"description"`
	Expect      string `json:"expect"`
}

type testResult struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Group    string `json:"group"`
	Pass     bool   `json:"pass"`
	Expect   string `json:"expect"`
	Got      string `json:"got"`
	Detail   string `json:"detail,omitempty"`
	Duration string `json:"duration"`
}

type runResponse struct {
	StartedAt string       `json:"startedAt"`
	Endpoints endpoints    `json:"endpoints"`
	Passed    int          `json:"passed"`
	Failed    int          `json:"failed"`
	Results   []testResult `json:"results"`
}

func main() {
	ep := endpoints{
		Arbitrator: envOr("ARBITRATOR_URL", "https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io"),
		ActiveHub:  envOr("ACTIVE_HUB_URL", "https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io"),
		StandbyHub: envOr("STANDBY_HUB_URL", "https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io"),
	}
	addr := envOr("LISTEN_ADDR", ":8090")

	static, err := fs.Sub(webFS, "web")
	if err != nil {
		fmt.Fprintf(os.Stderr, "embed: %v\n", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		b, err := webFS.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("GET /api/v1/cases", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"endpoints": ep, "cases": allCases()})
	})
	mux.HandleFunc("POST /api/v1/run", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, runAll(ep))
	})
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	fmt.Printf("demo-test-console listening on http://127.0.0.1%s\n", addr)
	fmt.Printf("  arbitrator: %s\n  active:     %s\n  standby:    %s\n", ep.Arbitrator, ep.ActiveHub, ep.StandbyHub)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}

func allCases() []testCase {
	return []testCase{
		{ID: "arb-health", Group: "Arbitrator", Name: "Health endpoint", Description: "GET /api/v1/health on hub arbitrator", Expect: `status == "ok"`},
		{ID: "arb-active", Group: "Arbitrator", Name: "cluster1-fis is active", Description: "X-Cluster-Name: cluster1-fis → role active, acceptTraffic true", Expect: `role=active, acceptTraffic=true`},
		{ID: "arb-standby", Group: "Arbitrator", Name: "cluster2-fis is standby", Description: "X-Cluster-Name: cluster2-fis → role standby, acceptTraffic false", Expect: `role=standby, acceptTraffic=false`},
		{ID: "hub1-health", Group: "Payment hubs", Name: "Active hub health", Description: "cluster1 payment-hub reports active", Expect: `role=active, acceptTraffic=true`},
		{ID: "hub2-health", Group: "Payment hubs", Name: "Standby hub health", Description: "cluster2 payment-hub reports standby", Expect: `role=standby, acceptTraffic=false`},
		{ID: "pay-active", Group: "Payments", Name: "Active accepts payment", Description: "POST /api/v1/payments on cluster1", Expect: `status=accepted (HTTP 2xx)`},
		{ID: "pay-standby", Group: "Payments", Name: "Standby refuses payment", Description: "POST /api/v1/payments on cluster2", Expect: `error=standby_or_fenced (HTTP 503)`},
	}
}

func runAll(ep endpoints) runResponse {
	client := &http.Client{
		Timeout: 12 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // lab OpenShift edge certs
		},
	}
	started := time.Now().UTC()
	out := runResponse{
		StartedAt: started.Format(time.RFC3339),
		Endpoints: ep,
		Results:   make([]testResult, 0, 7),
	}

	runners := map[string]func() testResult{
		"arb-health":  func() testResult { return checkArbHealth(client, ep) },
		"arb-active":  func() testResult { return checkArbRole(client, ep, "cluster1-fis", "active", true) },
		"arb-standby": func() testResult { return checkArbRole(client, ep, "cluster2-fis", "standby", false) },
		"hub1-health": func() testResult { return checkHubHealth(client, ep.ActiveHub, "active", true) },
		"hub2-health": func() testResult { return checkHubHealth(client, ep.StandbyHub, "standby", false) },
		"pay-active":  func() testResult { return checkPayAccept(client, ep.ActiveHub) },
		"pay-standby": func() testResult { return checkPayRefuse(client, ep.StandbyHub) },
	}

	for _, tc := range allCases() {
		start := time.Now()
		res := runners[tc.ID]()
		res.ID = tc.ID
		res.Name = tc.Name
		res.Group = tc.Group
		res.Expect = tc.Expect
		res.Duration = time.Since(start).Round(time.Millisecond).String()
		if res.Pass {
			out.Passed++
		} else {
			out.Failed++
		}
		out.Results = append(out.Results, res)
	}
	return out
}

func checkArbHealth(c *http.Client, ep endpoints) testResult {
	code, body, err := doGET(c, strings.TrimRight(ep.Arbitrator, "/")+"/api/v1/health", nil)
	if err != nil {
		return fail(err.Error(), "")
	}
	status, _ := jsonString(body, "status")
	pass := code == 200 && status == "ok"
	return result(pass, fmt.Sprintf("HTTP %d status=%q", code, status), truncate(body, 400))
}

func checkArbRole(c *http.Client, ep endpoints, cluster, wantRole string, wantAccept bool) testResult {
	code, body, err := doGET(c, strings.TrimRight(ep.Arbitrator, "/")+"/api/v1/state", map[string]string{
		"X-Cluster-Name": cluster,
	})
	if err != nil {
		return fail(err.Error(), "")
	}
	role, _ := jsonString(body, "datacenter", "role")
	accept, _ := jsonBool(body, "datacenter", "acceptTraffic")
	pass := code == 200 && role == wantRole && accept == wantAccept
	return result(pass, fmt.Sprintf("HTTP %d role=%s acceptTraffic=%v", code, role, accept), truncate(body, 500))
}

func checkHubHealth(c *http.Client, base, wantRole string, wantAccept bool) testResult {
	code, body, err := doGET(c, strings.TrimRight(base, "/")+"/health", nil)
	if err != nil {
		return fail(err.Error(), "")
	}
	role, _ := jsonString(body, "role")
	accept, _ := jsonBool(body, "acceptTraffic")
	pass := code == 200 && role == wantRole && accept == wantAccept
	return result(pass, fmt.Sprintf("HTTP %d role=%s acceptTraffic=%v", code, role, accept), truncate(body, 400))
}

func checkPayAccept(c *http.Client, base string) testResult {
	payload := fmt.Sprintf(`{"amount":7,"from":"console","to":"demo","paymentId":"console-%d"}`, time.Now().UnixNano())
	code, body, err := doPOST(c, strings.TrimRight(base, "/")+"/api/v1/payments", payload)
	if err != nil {
		return fail(err.Error(), "")
	}
	status, _ := jsonString(body, "status")
	pass := code >= 200 && code < 300 && status == "accepted"
	return result(pass, fmt.Sprintf("HTTP %d status=%q", code, status), truncate(body, 400))
}

func checkPayRefuse(c *http.Client, base string) testResult {
	payload := `{"amount":7,"from":"console","to":"demo"}`
	code, body, err := doPOST(c, strings.TrimRight(base, "/")+"/api/v1/payments", payload)
	if err != nil {
		return fail(err.Error(), "")
	}
	errCode, _ := jsonString(body, "error")
	pass := code == 503 && errCode == "standby_or_fenced"
	return result(pass, fmt.Sprintf("HTTP %d error=%q", code, errCode), truncate(body, 400))
}

func doGET(c *http.Client, url string, headers map[string]string) (int, string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(b), nil
}

func doPOST(c *http.Client, url, payload string) (int, string, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(payload))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(b), nil
}

func jsonString(raw string, path ...string) (string, bool) {
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return "", false
	}
	cur := v
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = m[p]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

func jsonBool(raw string, path ...string) (bool, bool) {
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return false, false
	}
	cur := v
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return false, false
		}
		cur, ok = m[p]
		if !ok {
			return false, false
		}
	}
	b, ok := cur.(bool)
	return b, ok
}

func result(pass bool, got, detail string) testResult {
	return testResult{Pass: pass, Got: got, Detail: detail}
}

func fail(got, detail string) testResult {
	return testResult{Pass: false, Got: got, Detail: detail}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
