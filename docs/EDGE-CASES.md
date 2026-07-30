# Edge-case test checklist

Practical cases for the FIS dual-site demo (arbitrator + payment hubs + Kafka/MM2).

**URLs (lab)**

| Component | URL |
|-----------|-----|
| Arbitrator | `https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io/` |
| cluster1 | `https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io/` |
| cluster2 | `https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io/` |

```bash
export ARBITRATOR_URL="https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io"
export C1="https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io"
export C2="https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io"
```

Before each suite: hub GUI → **Clear (live signals)** (or `PUT /api/v1/simulation` with `mode=none`). Confirm cluster1 **active**, cluster2 **standby**.

---

## Highest-value demos

These show the interesting failure modes most clearly.

| # | Case | Steps | Expect |
|---|------|-------|--------|
| H1 | Stale poll window | Mark cluster1 unreachable → immediately POST payment to `$C1` (before ~5s poll) | May still accept briefly; after next poll, refuses. Document the lag. |
| H2 | MM2 pause | Scale MM2 to 0 on both sites → pay on active → compare balances → scale MM2 back | Active updates; standby lags; catch-up after MM2 returns |
| H3 | Arbitrator restart mid-sim | Mark cluster1 unreachable → bounce arbitrator pod | Simulation is in-memory → clears; roles snap back to live/priority |
| H4 | Duplicate `paymentId` | POST same `paymentId` twice while active | Events may appear twice in Kafka; **balance apply once per id** — no double-debit |

```bash
# H1 sketch
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"unreachable","target":"cluster1-fis"}'
curl -sk -X POST "$C1/api/v1/payments" \
  -H 'Content-Type: application/json' \
  -d '{"paymentId":"stale-window-1","amount":1,"from":"alice","to":"bob"}' | jq .
sleep 6
curl -sk -X POST "$C1/api/v1/payments" \
  -H 'Content-Type: application/json' \
  -d '{"paymentId":"stale-window-2","amount":1,"from":"alice","to":"bob"}' | jq .
# expect second call: standby_or_fenced

# H4 sketch
PAY=dup-$(date +%s)
curl -sk -X POST "$C1/api/v1/payments" -H 'Content-Type: application/json' \
  -d "{\"paymentId\":\"$PAY\",\"amount\":10,\"from\":\"alice\",\"to\":\"bob\"}" | jq .
curl -sk -X POST "$C1/api/v1/payments" -H 'Content-Type: application/json' \
  -d "{\"paymentId\":\"$PAY\",\"amount\":10,\"from\":\"alice\",\"to\":\"bob\"}" | jq .
sleep 8
curl -sk "$C1/api/v1/balances" | jq .
curl -sk "$C2/api/v1/balances" | jq .
```

---

## Role / arbitrator

| # | Case | How | Expect | Pass? |
|---|------|-----|--------|-------|
| R1 | Standby rejects payments | POST `$C2/api/v1/payments` while standby | `standby_or_fenced` | ☐ |
| R2 | Mesh down, no failover | Hub: **Submariner mesh down** | cluster1 stays active; both reachable; `partitionDetected` | ☐ |
| R3 | Failover to cluster2 | **Mark cluster1 unreachable** | c1 unreachable, c2 active | ☐ |
| R4 | Mark standby unreachable | **Mark cluster2 unreachable** | c1 stays active; c2 unreachable | ☐ |
| R5 | Clear after failover | **Clear (live signals)** | priority active back on c1 | ☐ |
| R6 | Rapid flip | unreachable → clear → unreachable | roles follow within ~1 poll; never dual-active | ☐ |
| R7 | Missing `X-Cluster-Name` | `GET $ARBITRATOR_URL/api/v1/state` with no header | not treated as priority active; safe/unknown path | ☐ |
| R8 | Unknown cluster name | `X-Cluster-Name: other-cluster` | standby / `unknown_cluster`; `acceptTraffic=false` | ☐ |

```bash
# R1
curl -sk -X POST "$C2/api/v1/payments" \
  -H 'Content-Type: application/json' \
  -d '{"amount":5,"from":"alice","to":"bob"}' | jq .

# R7 / R8
curl -sk "$ARBITRATOR_URL/api/v1/state" | jq '.datacenter'
curl -sk -H 'X-Cluster-Name: other-cluster' "$ARBITRATOR_URL/api/v1/state" | jq '.datacenter'
```

---

## Payments / ledger

| # | Case | How | Expect | Pass? |
|---|------|-----|--------|-------|
| P1 | Tx visible on both sites | Pay on active; watch standby **Transactions** | `local` on active; `replicated` on standby within a few seconds | ☐ |
| P2 | Balances match | Same payment; compare **Balances** / `/api/v1/balances` | Same users and totals after MM2 catch-up | ☐ |
| P3 | Zero amount | POST `amount: 0` | Payment may accept; ledger skips amount `0` | ☐ |
| P4 | Duplicate `paymentId` | See H4 | No double-debit on balances | ☐ |
| P5 | Unknown users | `from`/`to` new names | Seeded at 1000, then transfer applied | ☐ |
| P6 | Refuse doesn’t move money | Pay on standby; note balances before/after | Balances unchanged | ☐ |

```bash
# P1 / P2
PAY=edge-$(date +%s)
BEFORE=$(curl -sk "$C1/api/v1/balances")
curl -sk -X POST "$C1/api/v1/payments" -H 'Content-Type: application/json' \
  -d "{\"paymentId\":\"$PAY\",\"amount\":25,\"from\":\"alice\",\"to\":\"bob\"}" | jq .
sleep 10
curl -sk "$C1/api/v1/events" | jq --arg p "$PAY" '[.events[]|select(.paymentId==$p)]'
curl -sk "$C2/api/v1/events" | jq --arg p "$PAY" '[.events[]|select(.paymentId==$p)]'
curl -sk "$C1/api/v1/balances" | jq .
curl -sk "$C2/api/v1/balances" | jq .
```

---

## Failover timing

| # | Case | How | Expect | Pass? |
|---|------|-----|--------|-------|
| F1 | Pay then failover | Submit on c1 → mark c1 unreachable | Payment in Kafka; c2 shows replicated + balance; new pays only on c2 | ☐ |
| F2 | Pay during role flip | Submit while changing simulation | Exactly one site accepts; other refuses | ☐ |
| F3 | Stale role window | See H1 | Up to ~`ARBITRATOR_POLL` lag on old active | ☐ |
| F4 | New active after failover | After R3, POST to `$C2` | Accepted; appears on c1 as replicated when MM2 healthy | ☐ |

```bash
# F1 / F4
PAY=fail-$(date +%s)
curl -sk -X POST "$C1/api/v1/payments" -H 'Content-Type: application/json' \
  -d "{\"paymentId\":\"$PAY\",\"amount\":7,\"from\":\"alice\",\"to\":\"bob\"}" | jq .
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"unreachable","target":"cluster1-fis"}'
sleep 6
curl -sk "$C1/health" | jq '{role,acceptTraffic}'
curl -sk "$C2/health" | jq '{role,acceptTraffic}'
curl -sk -X POST "$C2/api/v1/payments" -H 'Content-Type: application/json' \
  -d '{"amount":3,"from":"bob","to":"alice"}' | jq .
```

---

## Infra / recovery

| # | Case | How | Expect | Pass? |
|---|------|-----|--------|-------|
| I1 | Arbitrator restart | Bounce arbitrator deployment mid-simulation | Simulation cleared; live roles restored | ☐ |
| I2 | Payment-hub restart | Bounce `payment-hub` on one site | UI briefly empty; txs/balances rebuild from Kafka | ☐ |
| I3 | MM2 lag / stop | See H2 | Standby diverges then catches up | ☐ |
| I4 | Arbitrator unreachable | Bad `ARBITRATOR_URL` or block route (advanced) | Hubs keep **last** cached role (stale fencing risk) | ☐ |

```bash
# I1 (hub kubeconfig)
export KUBECONFIG="${HUB_KUBECONFIG:-$HOME/Downloads/rose-fis-kubeconfig.yaml}"
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"unreachable","target":"cluster1-fis"}'
oc -n open-cluster-management rollout restart deployment/akka-split-brain-arbitrator
oc -n open-cluster-management rollout status deployment/akka-split-brain-arbitrator
sleep 5
curl -sk "$ARBITRATOR_URL/api/v1/simulation" | jq .
curl -sk "$ARBITRATOR_URL/api/v1/overview" | jq '{sim:.simulation, sites:[.sites[]|{name,role}]}'

# I2 (cluster1 kubeconfig)
export KUBECONFIG="${CLUSTER1_KUBECONFIG:-$HOME/Downloads/cluster1-fis-kubeconfig.yaml}"
oc -n payment-hub rollout restart deployment/payment-hub
oc -n payment-hub rollout status deployment/payment-hub
sleep 15
curl -sk "$C1/api/v1/balances" | jq .
```

**MM2 pause (H2 / I3)** — adjust names if your MM2 resource differs:

```bash
# cluster1
export KUBECONFIG="${CLUSTER1_KUBECONFIG:-$HOME/Downloads/cluster1-fis-kubeconfig.yaml}"
oc -n kafka scale kafkamirrormaker2/mm2-to-cluster2-fis --replicas=0
# cluster2
export KUBECONFIG="${CLUSTER2_KUBECONFIG:-$HOME/Downloads/cluster2-fis-kubeconfig.yaml}"
oc -n kafka scale kafkamirrormaker2/mm2-to-cluster1-fis --replicas=0
# pay on active, compare balances, then scale --replicas=1 on both
```

---

## Suggested run order (demo day)

1. **R1, P1, P2** — baseline happy path  
2. **R2** — mesh down without failover  
3. **R3, F4, F1** — failover + continue on peer  
4. **H1** — call out poll lag  
5. **H4 / P4** — idempotent ledger  
6. **H3 / I1** — simulation not durable  
7. **H2** — replication lag (optional, needs MM2 scale)  
8. **R5** — restore normal  

---

## Known limitations (not bugs, but call them out)

| Topic | Behavior |
|-------|----------|
| Poll lag | Role changes apply on next `ARBITRATOR_POLL` (~5s), not instantly |
| Simulation durability | Lost on arbitrator process restart |
| Stale fencing | If arbitrator is unreachable, hubs keep last role |
| Balance seed | New users start at `STARTING_BALANCE` (default 1000) |
| Dual-active | Should not happen while both hubs can reach arbitrator; risk only with stale cache + arb down |

---

## Related docs

- [ARCHITECTURE-README.md](./ARCHITECTURE-README.md) — system design + Mermaid  
- [arbitrator-api-for-payment-hub.md](./arbitrator-api-for-payment-hub.md) — arbitrator APIs  
- Root [README.md](../README.md) — deploy + live URLs  
