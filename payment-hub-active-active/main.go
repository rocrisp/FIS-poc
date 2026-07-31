// Payment hub (active-active): independent site process that:
// 1) polls the ACM active-active arbitrator (both healthy sites acceptTraffic=true)
// 2) accepts payments only when acceptTraffic=true AND ledger catch-up is complete
//    AND the payer's home site matches this cluster (unless sole-active / anyPayer)
// 3) writes lifecycle events to local Kafka for MM2 sync to the peer site
// 4) consumes local + mirrored Kafka topics so both sites show the same ledger
// 5) serves a small site dashboard UI
//
// Safe rule: a site that was fenced/down must not accept new payments until Kafka
// catch-up finishes (ledgerReady). See docs/CATCH-UP-AND-FENCE-EPOCH.md.
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
		SoleActive    bool   `json:"soleActive"`
	} `json:"datacenter"`
	Partition struct {
		Detected       bool     `json:"detected"`
		ActivePeers    []string `json:"activePeers"`
		SoleActiveSite string   `json:"soleActiveSite"`
		WriteMode      string   `json:"writeMode"`
		FallbackActive string   `json:"fallbackActive"`
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
// Each site applies the same Kafka stream so both active peers stay aligned.
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
	mu             sync.RWMutex
	role           string
	acceptTraffic  bool
	reason         string
	partitioned    bool
	activePeers    []string
	simMode        string
	hubReachable   bool
	soleActive     bool
	writeMode      string
	fallbackActive string
	anyPayer       bool // sole-active or hub-down fallback: skip letter affinity
}

func (s *siteRole) snapshot() (role string, accept bool, reason string, partitioned bool, peers []string, sim string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	peers = append([]string(nil), s.activePeers...)
	return s.role, s.acceptTraffic, s.reason, s.partitioned, peers, s.simMode
}

func (s *siteRole) policy() (accept, anyPayer, hubOK, sole bool, writeMode, reason string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.acceptTraffic, s.anyPayer, s.hubReachable, s.soleActive, s.writeMode, s.reason
}

// update applies arbitrator state. fenced is true when this site must not take
// payments (acceptTraffic false) — callers should Invalidate the catch-up gate.
func (s *siteRole) update(st arbitratorState) (fenced bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hubReachable = st.Simulation.Mode != "hub-down"
	s.role = st.Datacenter.Role
	s.acceptTraffic = st.Datacenter.AcceptTraffic
	s.reason = st.Datacenter.Reason
	s.partitioned = st.Partition.Detected
	s.activePeers = append([]string(nil), st.Partition.ActivePeers...)
	s.simMode = st.Simulation.Mode
	s.soleActive = st.Datacenter.SoleActive // only this site — do not infer from global writeMode
	s.writeMode = st.Partition.WriteMode
	if st.Simulation.Mode == "hub-down" {
		s.writeMode = "hub-unreachable-fallback"
	} else if s.writeMode == "" {
		if s.soleActive {
			s.writeMode = "sole-active"
		} else {
			s.writeMode = "active-active"
		}
	}
	s.fallbackActive = st.Partition.FallbackActive
	// When the hub says this site is the only writer, accept any payer.
	s.anyPayer = s.acceptTraffic && s.soleActive
	return !s.acceptTraffic
}

// applyHubUnreachableFallback: payment hubs cannot reach the arbitrator.
// Policy: cluster1 (fallback) stays sole active; cluster2 refuses until hub returns.
func (s *siteRole) applyHubUnreachableFallback(clusterName, fallbackActive string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hubReachable = false
	s.simMode = ""
	s.partitioned = true
	s.writeMode = "hub-unreachable-fallback"
	s.fallbackActive = fallbackActive
	s.activePeers = nil
	if clusterName == fallbackActive {
		s.role = "active"
		s.acceptTraffic = true
		s.soleActive = true
		s.anyPayer = true
		s.reason = "hub_unreachable_fallback_active"
		s.activePeers = []string{clusterName}
	} else {
		s.role = "standby"
		s.acceptTraffic = false
		s.soleActive = false
		s.anyPayer = false
		s.reason = "hub_unreachable_cluster1_active"
	}
}

// homeSite returns the owning cluster for the payer account (from).
// Demo affinity: first letter a–m → cluster1-fis, n–z → cluster2-fis.
func homeSite(account string) string {
	account = strings.TrimSpace(strings.ToLower(account))
	if account == "" {
		return ""
	}
	c := account[0]
	if c >= 'n' && c <= 'z' {
		return "cluster2-fis"
	}
	return "cluster1-fis"
}

func main() {
	clusterName := envOr("CLUSTER_NAME", "cluster1-fis")
	peerCluster := envOr("PEER_CLUSTER", defaultPeer(clusterName))
	fallbackActive := envOr("HUB_FALLBACK_ACTIVE", "cluster1-fis")
	arbitratorURL := strings.TrimRight(envOr("ARBITRATOR_URL", "http://localhost:8080"), "/")
	kafkaBootstrap := envOr("KAFKA_BOOTSTRAP", "kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")
	listen := envOr("LISTEN_ADDR", ":8080")
	pollEvery := durationOr("ARBITRATOR_POLL", 5*time.Second)

	producer, err := newProducer(kafkaBootstrap)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	role := &siteRole{role: "unknown", acceptTraffic: false, fallbackActive: fallbackActive}
	events := newEventLog()
	balances := newLedger(floatOr("STARTING_BALANCE", 1000))
	traffic := &trafficLog{}
	gate := newCatchUpGate(durationOr("CATCH_UP_IDLE", 2*time.Second))
	gate.Invalidate("catching_up_starting")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	insecureTLS := envOr("ARBITRATOR_INSECURE_SKIP_VERIFY", "true") == "true"
	go pollArbitrator(ctx, arbitratorURL, clusterName, fallbackActive, role, gate, traffic, pollEvery, insecureTLS)
	go consumeTransactions(ctx, kafkaBootstrap, clusterName, peerCluster, events, balances, gate)

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
		r, accept, reason, partitioned, peers, sim := role.snapshot()
		_, anyPayer, hubOK, sole, writeMode, _ := role.policy()
		ledgerReady, catchReason, _, catchParts, catchDone := gate.snapshot()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":         "ok",
			"mode":           "active-active",
			"cluster":        clusterName,
			"peerCluster":    peerCluster,
			"role":           r,
			"acceptTraffic":  accept,
			"ledgerReady":    ledgerReady,
			"catchUpReason":  catchReason,
			"catchUpParts":   catchParts,
			"catchUpDone":    catchDone,
			"reason":         reason,
			"partitioned":    partitioned,
			"activePeers":    peers,
			"simulation":     sim,
			"hubReachable":   hubOK,
			"soleActive":     sole,
			"writeMode":      writeMode,
			"anyPayer":       anyPayer,
			"fallbackActive": fallbackActive,
			"affinity":       "a-m→cluster1, n-z→cluster2 when both active; any payer when sole-active or hub-down fallback",
		})
	})
	mux.HandleFunc("GET /api/v1/affinity", func(w http.ResponseWriter, _ *http.Request) {
		_, anyPayer, hubOK, sole, writeMode, reason := role.policy()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"mode":           "active-active",
			"cluster":        clusterName,
			"hubReachable":   hubOK,
			"soleActive":     sole,
			"writeMode":      writeMode,
			"anyPayer":       anyPayer,
			"reason":         reason,
			"fallbackActive": fallbackActive,
			"rule":           "letter affinity when both sites active; any payer when this site is sole-active or hub-unreachable fallback on cluster1",
			"examples": map[string]string{
				"alice": homeSite("alice"),
				"bob":   homeSite("bob"),
				"nancy": homeSite("nancy"),
				"oscar": homeSite("oscar"),
			},
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
		accept, anyPayer, _, _, _, reason := role.policy()
		ledgerReady, catchReason, _, _, _ := gate.snapshot()
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
				Detail: reason,
				Amount: req.Amount,
			})
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":   "not_accepting",
				"message": reason,
				"cluster": clusterName,
			})
			return
		}
		if !ledgerReady {
			events.add(siteEvent{
				TS:     time.Now().UTC().Format(time.RFC3339),
				Status: "refused",
				Origin: "ui",
				Source: clusterName,
				Detail: catchReason,
				Amount: req.Amount,
			})
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":   "catching_up",
				"message": "site must finish Kafka ledger catch-up before accepting payments",
				"detail":  catchReason,
				"cluster": clusterName,
			})
			return
		}
		home := homeSite(req.From)
		if home == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error":   "missing_from",
				"message": "from account is required",
			})
			return
		}
		// Letter affinity only when both sites are active; sole-active / hub-down fallback accepts any payer.
		if !anyPayer && home != clusterName {
			events.add(siteEvent{
				TS:     time.Now().UTC().Format(time.RFC3339),
				Status: "refused",
				Origin: "ui",
				Source: clusterName,
				Detail: "wrong_home_site:" + home,
				Amount: req.Amount,
			})
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":    "wrong_home_site",
				"message":  fmt.Sprintf("payer %q is homed on %s; submit on that site", req.From, home),
				"cluster":  clusterName,
				"homeSite": home,
				"from":     req.From,
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

	log.Printf("payment-hub-aa mode=active-active cluster=%s peer=%s arbitrator=%s kafka=%s listen=%s", clusterName, peerCluster, arbitratorURL, kafkaBootstrap, listen)
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

func consumeTransactions(ctx context.Context, bootstrap, clusterName, peer string, events *eventLog, balances *ledger, gate *catchUpGate) {
	topics := consumeTopics(clusterName, peer)
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Version = sarama.V3_3_0_0

	// Separate consumer group from the active-passive demo.
	group := envOr("KAFKA_CONSUMER_GROUP", fmt.Sprintf("payment-hub-aa-%s-ledger", clusterName))
	brokers := strings.Split(bootstrap, ",")

	// Balances are in-memory only. On every process start, reset the consumer group
	// so OffsetOldest re-reads full topic history. Otherwise a rolling deploy commits
	// offsets on pod A and pod B resumes mid-stream with an empty ledger.
	if envOr("RESET_LEDGER_ON_START", "true") == "true" {
		gate.Invalidate("catching_up_rebuild")
		if err := resetConsumerGroup(brokers, group, cfg); err != nil {
			log.Printf("kafka reset consumer group %s: %v (continuing)", group, err)
		} else {
			log.Printf("kafka consumer group %s reset — rebuilding ledger from topic start", group)
		}
	}

	client, err := sarama.NewConsumerGroup(brokers, group, cfg)
	if err != nil {
		log.Printf("kafka consumer group: %v (retrying)", err)
		go retryConsume(ctx, bootstrap, clusterName, peer, events, balances, gate)
		return
	}
	defer client.Close()

	handler := &txHandler{cluster: clusterName, events: events, balances: balances, gate: gate}
	log.Printf("consuming kafka topics: %v group=%s", topics, group)
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

func resetConsumerGroup(brokers []string, group string, cfg *sarama.Config) error {
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()
	err = admin.DeleteConsumerGroup(group)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "unknown consumer group") {
		return err
	}
	return nil
}

func retryConsume(ctx context.Context, bootstrap, clusterName, peer string, events *eventLog, balances *ledger, gate *catchUpGate) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			consumeTransactions(ctx, bootstrap, clusterName, peer, events, balances, gate)
			return
		}
	}
}

type txHandler struct {
	cluster  string
	events   *eventLog
	balances *ledger
	gate     *catchUpGate
}

func (h *txHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *txHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *txHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.gate.notePartition(claim.Topic(), claim.Partition(), claim.InitialOffset(), claim.HighWaterMarkOffset())
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sess.Context().Done():
			return nil
		case <-ticker.C:
			// Refresh HWM; if Invalidate cleared parts, re-register this claim.
			h.gate.notePartition(claim.Topic(), claim.Partition(), claim.InitialOffset(), claim.HighWaterMarkOffset())
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.handleMessage(msg)
			h.gate.noteOffset(msg.Topic, msg.Partition, msg.Offset, claim.HighWaterMarkOffset())
			sess.MarkMessage(msg, "")
		}
	}
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

func pollArbitrator(ctx context.Context, baseURL, cluster, fallbackActive string, role *siteRole, gate *catchUpGate, traffic *trafficLog, every time.Duration, insecureTLS bool) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if insecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // demo against OpenShift edge routes
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	var wasAccept bool
	fetch := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/state", nil)
		if err != nil {
			return
		}
		req.Header.Set("X-Cluster-Name", cluster)
		traffic.add("out", "GET "+baseURL+"/api/v1/state X-Cluster-Name="+cluster)
		resp, err := client.Do(req)
		if err != nil {
			role.applyHubUnreachableFallback(cluster, fallbackActive)
			accept, _, _, _, _, _ := role.policy()
			if !accept {
				gate.Invalidate("catching_up_fenced")
			} else if !wasAccept && accept {
				gate.Invalidate("catching_up_after_fence")
			}
			wasAccept = accept
			traffic.add("in", "ERROR "+err.Error()+" → hub-unreachable fallback (cluster1 sole active)")
			log.Printf("arbitrator poll error: %v — fallback active=%s", err, fallbackActive)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			role.applyHubUnreachableFallback(cluster, fallbackActive)
			accept, _, _, _, _, _ := role.policy()
			if !accept {
				gate.Invalidate("catching_up_fenced")
			} else if !wasAccept && accept {
				gate.Invalidate("catching_up_after_fence")
			}
			wasAccept = accept
			traffic.add("in", fmt.Sprintf("HTTP %d → hub-unreachable fallback", resp.StatusCode))
			return
		}
		var st arbitratorState
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			role.applyHubUnreachableFallback(cluster, fallbackActive)
			accept, _, _, _, _, _ := role.policy()
			if !accept {
				gate.Invalidate("catching_up_fenced")
			} else if !wasAccept && accept {
				gate.Invalidate("catching_up_after_fence")
			}
			wasAccept = accept
			traffic.add("in", fmt.Sprintf("HTTP %d decode_error: %v → hub-unreachable fallback", resp.StatusCode, err))
			log.Printf("arbitrator decode error: %v", err)
			return
		}
		fenced := role.update(st)
		accept, _, _, _, _, _ := role.policy()
		if fenced {
			gate.Invalidate("catching_up_fenced")
		} else if !wasAccept && accept {
			// Returned to open: must re-prove catch-up before taking money.
			gate.Invalidate("catching_up_after_fence")
		}
		wasAccept = accept
		traffic.add("in", fmt.Sprintf(
			"HTTP %d ← role=%s accept=%v sole=%v writeMode=%s reason=%s peers=%v sim=%s",
			resp.StatusCode, st.Datacenter.Role, st.Datacenter.AcceptTraffic, st.Datacenter.SoleActive,
			st.Partition.WriteMode, st.Datacenter.Reason, st.Partition.ActivePeers, st.Simulation.Mode,
		))
		log.Printf("role=%s accept=%v sole=%v writeMode=%s reason=%s", st.Datacenter.Role, st.Datacenter.AcceptTraffic, st.Datacenter.SoleActive, st.Partition.WriteMode, st.Datacenter.Reason)
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
