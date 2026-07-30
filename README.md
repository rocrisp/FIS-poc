# FIS-poc — dual-site payment active/standby demo

Two independent payment sites (`cluster1-fis`, `cluster2-fis`) connected by Submariner, with:

1. **ACM arbitrator** (hub / `rose-fis`) — picks **active** vs **standby** when the mesh fails  
2. **Kafka + MirrorMaker 2** — async event sync (`payment-instructions`, `payment-lifecycle`)  
3. **payment-hub-demo** — accepts payments only when `acceptTraffic=true`

**Architecture (Mermaid + tech map):** [docs/ARCHITECTURE-README.md](docs/ARCHITECTURE-README.md)

```text
                    rose-fis (ACM hub)
                 akka-split-brain-arbitrator
                    /                  \
                   /                    \
          cluster1-fis                 cluster2-fis
          role=active                  role=standby
          payment-hub + Kafka   <---MM2--->  payment-hub + Kafka
```

## Repo layout

| Path | Purpose |
|------|---------|
| `akka-split-brain-arbitrator/` | Hub status API + simulation GUI + Submariner/ManagedCluster monitor |
| `payment-hub-demo/` | Per-site payment API + ACTIVE/STANDBY dashboard |
| `docs/ARCHITECTURE-README.md` | How it works — technologies + Mermaid diagrams |
| `docs/EDGE-CASES.md` | Edge-case test checklist for demo / QA |
| `docs/arbitrator-api-for-payment-hub.md` | Arbitrator APIs payment hubs can query |
| `demo-test-console/` | Optional local curl-based checks (prefer hub/site GUIs) |
| `k8s/cluster1-fis/kafka/` | Active-site Kafka/MM2 (SNO-sized) |
| `k8s/cluster2-fis/kafka/` | Standby-site Kafka/MM2 |
| `k8s/acm/` | ACM/Hive provisioning references |
| `scripts/` | Deploy helpers |

## Deploy order

```bash
# 0) Kubeconfigs
export HUB_KUBECONFIG=~/Downloads/rose-fis-kubeconfig.yaml
export CLUSTER1_KUBECONFIG=~/Downloads/cluster1-fis-kubeconfig.yaml
export CLUSTER2_KUBECONFIG=~/Downloads/cluster2-fis-kubeconfig.yaml

# 1) Arbitrator on hub
./scripts/deploy-arbitrator.sh
export ARBITRATOR_URL="https://$(oc --kubeconfig "$HUB_KUBECONFIG" -n open-cluster-management get route akka-split-brain-arbitrator -o jsonpath='{.spec.host}')"

# 2) Kafka on both sites
./scripts/deploy-kafka-site.sh cluster1-fis
./scripts/deploy-kafka-site.sh cluster2-fis
./scripts/wire-mirrormaker2.sh

# 3) Payment hubs
./scripts/deploy-payment-hub.sh cluster1-fis
./scripts/deploy-payment-hub.sh cluster2-fis
```

## Live demo endpoints (lab)

| Component | URL |
|-----------|-----|
| Arbitrator | `https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io` |
| Active payment-hub | `https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io` |
| Standby payment-hub | `https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io` |

## Demo checks (GUI)

Open these in a browser:

1. **Hub arbitrator** — overview + simulation controls  
   `https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io/`
2. **cluster1 payment hub** — big ACTIVE/STANDBY + payment form  
   `https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io/`
3. **cluster2 payment hub** — same UI on the peer site  
   `https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io/`

### Failover walkthrough

1. Confirm cluster1 is **ACTIVE**, cluster2 **STANDBY**; submit a payment on cluster1.
2. On the hub GUI: **Mark cluster1 unreachable**.
3. Within a few seconds cluster2 becomes **ACTIVE**, cluster1 **UNREACHABLE**.
4. Submit a payment on cluster2; cluster1 form stays disabled / refuses.
5. Hub GUI: **Clear (live signals)** to restore priority active on cluster1.

### API (optional)

```bash
curl -sk "$ARBITRATOR_URL/api/v1/overview" | jq .
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"unreachable","target":"cluster1-fis"}' | jq .
curl -sk -X PUT "$ARBITRATOR_URL/api/v1/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"none"}' | jq .
```

## Roles

| Role | Meaning |
|------|---------|
| `active` | Accept new payments |
| `standby` | Healthy, replicate events, refuse new payments |
| `unreachable` | Hub cannot reach that managed cluster API |

## Notes

- Payment hubs skip TLS verify against the hub OpenShift Route (`ARBITRATOR_INSECURE_SKIP_VERIFY=true`) for lab certs.
- MM2 uses `DefaultReplicationPolicy`: mirrored topics appear as `cluster1-fis.payment-instructions` (etc.) on the peer. Topic regexes are anchored so bidirectional MM2 cannot loop.
- Strimzi install rewrites upstream `myproject` RoleBinding subjects to `kafka`.
