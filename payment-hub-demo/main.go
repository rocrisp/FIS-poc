// Payment hub demo: independent site process that:
// 1) polls the ACM arbitrator for active/standby
// 2) accepts payment instructions only when acceptTraffic=true
// 3) writes lifecycle events to local Kafka for MM2 sync to the standby site
// 4) consumes local + mirrored Kafka topics so both sites show the same transactions
// 5) serves a small site dashboard UI
package main

import (
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

//go:embed web/*
var uiRoot embed.FS

type arbitratorState struct {
	Version    int64 `json:"version"`
	Datacenter struct {
		Name          string `json:"name"`
		Role          string `json:"role"`
		AcceptTraffic bool   `json:"acceptTraffic"`
		Reason        string `json:"reason"`
	} `json:"datacenter"`
	Partition struct {
		Detected          bool   `json:"detected"`
		ActiveDatacenter  string `json:"activeDatacenter"`
		StandbyDatacenter string `json:"standbyDatacenter"`
	} `json:"partition"`
	Simulation struct {
		Mode   string `json:"mode"`
		Target string `json:"target"`
	} `json:"simulation"`
}

type paymentRequest struct {
	PaymentID string  `json:"paymentId"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
	From      string  `json:"from"`
	To        string  `json:"to"`
}

type kafkaPaymentEvent struct {
	Type      string  `json:"type"`
	PaymentID string  `json:"paymentId"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
	From      string  `json:"from"`
	To        string  `json:"to"`
	Status    string  `json:"status"`
	Site      string  `json:"site"`
	TS        string  `json:"ts"`
}

type siteEvent struct {
	TS        string  `json:"ts"`
	Status    string  `json:"status"` // accepted|validated|refused|replicated
	Origin    string  `json:"origin"` // local|replicated|ui
	Source    string  `json:"source,omitempty"`
	PaymentID string  `json:"paymentId,omitempty"`
	Detail    string  `json:"detail,omitempty"`
	Amount    float64 `json:"amount,omitempty"`
	Topic     string  `json:"topic,omitempty"`
}

type eventLog struct {
	mu    sync.RWMutex
	items []siteEvent
	seen  map[string]struct{}
}

func newEventLog() *eventLog {
	return &eventLog{seen: make(map[string]struct{})}
}

func (e *eventLog) add(ev siteEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := ev.PaymentID + "|" + ev.Status + "|" + ev.Origin + "|" + ev.Source
	if ev.PaymentID != "" {
		if _, ok := e.seen[key]; ok {
			return
		}
		e.seen[key] = struct{}{}
	}
	e.items = append([]siteEvent{ev}, e.items...)
	if len(e.items) > 80 {
		for _, drop := range e.items[80:] {
			delete(e.seen, drop.PaymentID+"|"+drop.Status+"|"+drop.Origin+"|"+drop.Source)
		}
		e.items = e.items[:80]
	}
}

func (e *eventLog) list() []siteEvent {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]siteEvent, len(e.items))
	copy(out, e.items)
	return out
}

// ledger derives per-user balances from validated payment lifecycle events.
// Each site applies the same Kafka stream, so active and standby stay aligned.
type userBalance struct {
	User     string  `json:"user"`
	Balance  float64 `json:"balance"`
	Currency string  `json:"currency"`
}

type ledger struct {
	mu       sync.RWMutex
	balances map[string]float64
	applied  map[string]struct{} // paymentId applied once
	starting float64
	currency string
}

func newLedger(starting float64) *ledger {
	return &ledger{
		balances: make(map[string]float64),
		applied:  make(map[string]struct{}),
		starting: starting,
		currency: "USD",
	}
}

func (l *ledger) ensureUser(user string) {
	if user == "" {
		return
	}
	if _, ok := l.balances[user]; !ok {
		l.balances[user] = l.starting
	}
}

// applyValidated debits from and credits to once per paymentId.
func (l *ledger) applyValidated(paymentID, from, to string, amount float64, currency string) bool {
	if paymentID == "" || amount == 0 || (from == "" && to == "") {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.applied[paymentID]; ok {
		return false
	}
	l.applied[paymentID] = struct{}{}
	if currency != "" {
		l.currency = currency
	}
	l.ensureUser(from)
	l.ensureUser(to)
	if from != "" {
		l.balances[from] -= amount
	}
	if to != "" {
		l.balances[to] += amount
	}
	return true
}

func (l *ledger) list() []userBalance {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]userBalance, 0, len(l.balances))
	for user, bal := range l.balances {
		out = append(out, userBalance{User: user, Balance: bal, Currency: l.currency})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].User < out[j].User })
	return out
}

type trafficEntry struct {
	TS      string `json:"ts"`
	Dir     string `json:"dir"` // in|out
	Summary string `json:"summary"`
}

type trafficLog struct {
	mu    sync.RWMutex
	items []trafficEntry
}

func (t *trafficLog) add(dir, summary string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.items = append([]trafficEntry{{
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
		Dir:     dir,
		Summary: summary,
	}}, t.items...)
	if len(t.items) > 200 {
		t.items = t.items[:200]
	}
}

func (t *trafficLog) list() []trafficEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]trafficEntry, len(t.items))
	copy(out, t.items)
	return out
}

type siteRole struct {
	mu            sync.RWMutex
	role          string
	acceptTraffic bool
	reason        string
	partitioned   bool
	activeSite    string
	simMode       string
}

func (s *siteRole) snapshot() (role string, accept bool, reason string, partitioned bool, active, sim string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role, s.acceptTraffic, s.reason, s.partitioned, s.activeSite, s.simMode
}

func (s *siteRole) update(st arbitratorState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = st.Datacenter.Role
	s.acceptTraffic = st.Datacenter.AcceptTraffic
	s.reason = st.Datacenter.Reason
	s.partitioned = st.Partition.Detected
	s.activeSite = st.Partition.ActiveDatacenter
	s.simMode = st.Simulation.Mode
}

func main() {
	clusterName := envOr("CLUSTER_NAME", "cluster1-fis")
	peerCluster := envOr("PEER_CLUSTER", defaultPeer(clusterName))
	arbitratorURL := strings.TrimRight(envOr("ARBITRATOR_URL", "http://localhost:8080"), "/")
	kafkaBootstrap := envOr("KAFKA_BOOTSTRAP", "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")
	listen := envOr("LISTEN_ADDR", ":8080")
	pollEvery := durationOr("ARBITRATOR_POLL", 5*time.Second)

	producer, err := newProducer(kafkaBootstrap)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	role := &siteRole{role: "unknown", acceptTraffic: false}
	events := newEventLog()
	balances := newLedger(floatOr("STARTING_BALANCE", 1000))
	traffic := &trafficLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	insecureTLS := envOr("ARBITRATOR_INSECURE_SKIP_VERIFY", "true") == "true"
	go pollArbitrator(ctx, arbitratorURL, clusterName, role, traffic, pollEvery, insecureTLS)
	go consumeTransactions(ctx, kafkaBootstrap, clusterName, peerCluster, events, balances)

	webFS, err := fs.Sub(uiRoot, "web")
	if err != nil {
		log.Fatalf("ui: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(webFS))))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		b, err := fs.ReadFile(webFS, "index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		r, accept, reason, partitioned, active, sim := role.snapshot()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":        "ok",
			"cluster":       clusterName,
			"peerCluster":   peerCluster,
			"role":          r,
			"acceptTraffic": accept,
			"reason":        reason,
			"partitioned":   partitioned,
			"activeSite":    active,
			"simulation":    sim,
		})
	})
	mux.HandleFunc("GET /api/v1/events", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"cluster": clusterName,
			"peer":    peerCluster,
			"events":  events.list(),
		})
	})
	mux.HandleFunc("GET /api/v1/balances", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"cluster":         clusterName,
			"startingBalance": floatOr("STARTING_BALANCE", 1000),
			"currency":        "USD",
			"balances":        balances.list(),
			"note":            "derived from validated payment lifecycle events (Kafka/MM2)",
		})
	})
	mux.HandleFunc("GET /api/v1/traffic", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"arbitrator": arbitratorURL,
			"entries":    traffic.list(),
		})
	})
	mux.HandleFunc("POST /api/v1/payments", func(w http.ResponseWriter, r *http.Request) {
		_, accept, _, _, _, _ := role.snapshot()
		var req paymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_body"})
			return
		}
		if !accept {
			events.add(siteEvent{
				TS:     time.Now().UTC().Format(time.RFC3339),
				Status: "refused",
				Origin: "ui",
				Source: clusterName,
				Detail: "standby_or_fenced",
				Amount: req.Amount,
			})
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":   "standby_or_fenced",
				"message": "this site is not active; refuse new payments",
				"cluster": clusterName,
			})
			return
		}
		if req.PaymentID == "" {
			req.PaymentID = fmt.Sprintf("pay-%d", time.Now().UnixNano())
		}
		if req.Currency == "" {
			req.Currency = "USD"
		}

		now := time.Now().UTC().Format(time.RFC3339)
		instruction := map[string]interface{}{
			"type":      "payment.instruction.created",
			"paymentId": req.PaymentID,
			"amount":    req.Amount,
			"currency":  req.Currency,
			"from":      req.From,
			"to":        req.To,
			"site":      clusterName,
			"ts":        now,
		}
		lifecycle := map[string]interface{}{
			"type":      "payment.lifecycle.validated",
			"paymentId": req.PaymentID,
			"status":    "validated",
			"amount":    req.Amount,
			"currency":  req.Currency,
			"from":      req.From,
			"to":        req.To,
			"site":      clusterName,
			"ts":        now,
		}
		if err := publish(producer, "payment-instructions", req.PaymentID, instruction); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if err := publish(producer, "payment-lifecycle", req.PaymentID, lifecycle); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// Optimistic local row; Kafka consumer also records local + peer sees replicated via MM2.
		events.add(siteEvent{
			TS:        now,
			Status:    "accepted",
			Origin:    "local",
			Source:    clusterName,
			PaymentID: req.PaymentID,
			Detail:    fmt.Sprintf("%s %.2f %s→%s", req.Currency, req.Amount, req.From, req.To),
			Amount:    req.Amount,
			Topic:     "payment-instructions",
		})
		writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"paymentId": req.PaymentID,
			"status":    "accepted",
			"site":      clusterName,
		})
	})

	server := &http.Server{Addr: listen, Handler: mux}
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("payment-hub-demo cluster=%s peer=%s arbitrator=%s kafka=%s listen=%s", clusterName, peerCluster, arbitratorURL, kafkaBootstrap, listen)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}

func defaultPeer(cluster string) string {
	switch cluster {
	case "cluster1-fis":
		return "cluster2-fis"
	case "cluster2-fis":
		return "cluster1-fis"
	default:
		return ""
	}
}

func consumeTopics(clusterName, peer string) []string {
	topics := []string{"payment-instructions", "payment-lifecycle"}
	if peer != "" {
		topics = append(topics,
			peer+".payment-instructions",
			peer+".payment-lifecycle",
		)
	}
	return topics
}

func consumeTransactions(ctx context.Context, bootstrap, clusterName, peer string, events *eventLog, balances *ledger) {
	topics := consumeTopics(clusterName, peer)
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Version = sarama.V3_3_0_0

	// New group so balances rebuild from topic history after this feature lands.
	group := fmt.Sprintf("payment-hub-demo-%s-bal1", clusterName)
	client, err := sarama.NewConsumerGroup(strings.Split(bootstrap, ","), group, cfg)
	if err != nil {
		log.Printf("kafka consumer group: %v (retrying)", err)
		go retryConsume(ctx, bootstrap, clusterName, peer, events, balances)
		return
	}
	defer client.Close()

	handler := &txHandler{cluster: clusterName, events: events, balances: balances}
	log.Printf("consuming kafka topics: %v", topics)
	for {
		if err := client.Consume(ctx, topics, handler); err != nil {
			log.Printf("kafka consume: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func retryConsume(ctx context.Context, bootstrap, clusterName, peer string, events *eventLog, balances *ledger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			consumeTransactions(ctx, bootstrap, clusterName, peer, events, balances)
			return
		}
	}
}

type txHandler struct {
	cluster  string
	events   *eventLog
	balances *ledger
}

func (h *txHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *txHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *txHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.handleMessage(msg)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (h *txHandler) handleMessage(msg *sarama.ConsumerMessage) {
	var ev kafkaPaymentEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return
	}
	if ev.PaymentID == "" {
		return
	}
	source := ev.Site
	if source == "" {
		source = "unknown"
	}
	origin := "local"
	status := "accepted"
	switch {
	case strings.HasPrefix(msg.Topic, h.cluster+"."):
		// shouldn't normally consume our own mirrored prefix on this site
		origin = "local"
	case strings.Contains(msg.Topic, ".payment-"):
		origin = "replicated"
		status = "replicated"
	default:
		origin = "local"
		if source != h.cluster {
			origin = "replicated"
			status = "replicated"
		}
	}
	isLifecycle := strings.Contains(ev.Type, "lifecycle") || ev.Status == "validated" || strings.Contains(msg.Topic, "payment-lifecycle")
	if isLifecycle {
		if origin == "replicated" {
			status = "replicated"
		} else {
			status = "validated"
		}
		// Apply balance once per payment from lifecycle (local or mirrored topic).
		h.balances.applyValidated(ev.PaymentID, ev.From, ev.To, ev.Amount, ev.Currency)
	}
	detail := ev.Type
	if ev.Currency != "" || ev.Amount != 0 {
		detail = fmt.Sprintf("%s %.2f %s→%s", ev.Currency, ev.Amount, ev.From, ev.To)
	} else if ev.Status != "" {
		detail = ev.Status
	}
	ts := ev.TS
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}
	h.events.add(siteEvent{
		TS:        ts,
		Status:    status,
		Origin:    origin,
		Source:    source,
		PaymentID: ev.PaymentID,
		Detail:    detail,
		Amount:    ev.Amount,
		Topic:     msg.Topic,
	})
}

func pollArbitrator(ctx context.Context, baseURL, cluster string, role *siteRole, traffic *trafficLog, every time.Duration, insecureTLS bool) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if insecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // demo against OpenShift edge routes
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	fetch := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/state", nil)
		if err != nil {
			return
		}
		req.Header.Set("X-Cluster-Name", cluster)
		traffic.add("out", "GET "+baseURL+"/api/v1/state X-Cluster-Name="+cluster)
		resp, err := client.Do(req)
		if err != nil {
			traffic.add("in", "ERROR "+err.Error())
			log.Printf("arbitrator poll error: %v", err)
			return
		}
		defer resp.Body.Close()
		var st arbitratorState
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			traffic.add("in", fmt.Sprintf("HTTP %d decode_error: %v", resp.StatusCode, err))
			log.Printf("arbitrator decode error: %v", err)
			return
		}
		role.update(st)
		traffic.add("in", fmt.Sprintf(
			"HTTP %d ← role=%s acceptTraffic=%v reason=%s partition=%v active=%s sim=%s",
			resp.StatusCode, st.Datacenter.Role, st.Datacenter.AcceptTraffic, st.Datacenter.Reason,
			st.Partition.Detected, st.Partition.ActiveDatacenter, st.Simulation.Mode,
		))
		log.Printf("role=%s acceptTraffic=%v partition=%v active=%s sim=%s", st.Datacenter.Role, st.Datacenter.AcceptTraffic, st.Partition.Detected, st.Partition.ActiveDatacenter, st.Simulation.Mode)
	}
	fetch()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fetch()
		}
	}
}

func newProducer(bootstrap string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	return sarama.NewSyncProducer(strings.Split(bootstrap, ","), cfg)
}

func publish(p sarama.SyncProducer, topic, key string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, _, err = p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	})
	return err
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
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

func durationOr(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func floatOr(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
