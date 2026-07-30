# FIS-poc — dual-site payment demos

Two independent payment sites (`cluster1-fis`, `cluster2-fis`) connected by Submariner, with an ACM hub arbitrator and Kafka + MirrorMaker 2.

This repo has **two parallel demos**:

| Mode | Stack | Behavior |
|------|-------|----------|
| **Active-passive** (original) | `akka-split-brain-arbitrator` + `payment-hub-demo` | One site accepts payments; peer is standby |
| **Active-active** (new) | `arbitrator-active-active` + `payment-hub-active-active` | Both healthy sites accept; **payer affinity** avoids double-spend |

Active-active details: [docs/ACTIVE-ACTIVE.md](docs/ACTIVE-ACTIVE.md)  
AP vs AA (arbitrator + apps): [docs/ACTIVE-ACTIVE-VS-ACTIVE-PASSIVE.md](docs/ACTIVE-ACTIVE-VS-ACTIVE-PASSIVE.md)  
Architecture (Mermaid + tech map): [docs/ARCHITECTURE-README.md](docs/ARCHITECTURE-README.md)

```text
                    rose-fis (ACM hub)
         AP arbitrator              AA arbitrator
                    /                  \
                   /                    \
          cluster1-fis                 cluster2-fis
          payment-hub (+ aa)    <---MM2--->  payment-hub (+ aa)
          + Kafka                              + Kafka
```

## Repo layout

| Path | Purpose |
|------|---------|
| `akka-split-brain-arbitrator/` | **Active-passive** hub API + simulation GUI |
| `payment-hub-demo/` | **Active-passive** per-site payment UI |
| `arbitrator-active-active/` | **Active-active** hub API + simulation GUI |
| `payment-hub-active-active/` | **Active-active** payment UI + account affinity |
| `docs/ACTIVE-ACTIVE.md` | AA behavior, affinity, deploy |
| `docs/ACTIVE-ACTIVE-VS-ACTIVE-PASSIVE.md` | What changed in the arbitrator for AA + supporting apps |
| `docs/ARCHITECTURE-README.md` | How the AP stack works (diagrams) |
| `docs/KAFKA-MIRRORMAKER2.md` | Kafka listeners, MM2 replication |
| `docs/EDGE-CASES.md` | Edge-case checklist (AP) |
| `docs/arbitrator-api-for-payment-hub.md` | Arbitrator APIs |
| `k8s/cluster1-fis/kafka/`, `k8s/cluster2-fis/kafka/` | Shared Kafka/MM2 |
| `scripts/` | Deploy helpers (`*-aa.sh` for active-active) |

---

## Active-passive deploy (original)

```bash
export HUB_KUBECONFIG=~/Downloads/rose-fis-kubeconfig.yaml
export CLUSTER1_KUBECONFIG=~/Downloads/cluster1-fis-kubeconfig.yaml
export CLUSTER2_KUBECONFIG=~/Downloads/cluster2-fis-kubeconfig.yaml

./scripts/deploy-arbitrator.sh
export ARBITRATOR_URL="https://$(oc --kubeconfig "$HUB_KUBECONFIG" -n open-cluster-management get route akka-split-brain-arbitrator -o jsonpath='{.spec.host}')"

./scripts/deploy-kafka-site.sh cluster1-fis
./scripts/deploy-kafka-site.sh cluster2-fis
./scripts/wire-mirrormaker2.sh

./scripts/deploy-payment-hub.sh cluster1-fis
./scripts/deploy-payment-hub.sh cluster2-fis
```

### AP lab URLs

| Component | URL |
|-----------|-----|
| Arbitrator | `https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io` |
| Active payment-hub | `https://payment-hub-payment-hub.apps.cluster1-fis.opdev.io` |
| Standby payment-hub | `https://payment-hub-payment-hub.apps.cluster2-fis.opdev.io` |

### AP failover walkthrough

1. Confirm cluster1 **ACTIVE**, cluster2 **STANDBY**; pay on cluster1.
2. Hub GUI: **Mark cluster1 unreachable**.
3. cluster2 becomes **ACTIVE**; pay on cluster2.
4. Hub GUI: **Clear (live signals)** to restore priority active on cluster1.

### AP roles

| Role | Meaning |
|------|---------|
| `active` | Accept new payments |
| `standby` | Healthy, replicate events, refuse new payments |
| `unreachable` | Hub cannot reach that managed cluster API |

---

## Active-active deploy (parallel)

Kafka/MM2 from above can be reused. AA apps use different namespaces/routes and do not replace AP.

```bash
./scripts/deploy-arbitrator-aa.sh
export ARBITRATOR_AA_URL="https://$(oc --kubeconfig "$HUB_KUBECONFIG" -n open-cluster-management get route fis-arbitrator-active-active -o jsonpath='{.spec.host}')"

ARBITRATOR_URL="$ARBITRATOR_AA_URL" ./scripts/deploy-payment-hub-aa.sh cluster1-fis
ARBITRATOR_URL="$ARBITRATOR_AA_URL" ./scripts/deploy-payment-hub-aa.sh cluster2-fis
```

### AA affinity (payer `from`)

- **a–m** → home `cluster1-fis` (e.g. `alice`, `bob`)
- **n–z** → home `cluster2-fis` (e.g. `nancy`, `oscar`)

Wrong home → HTTP 409 even when the site is active. See [docs/ACTIVE-ACTIVE.md](docs/ACTIVE-ACTIVE.md).

---

## Notes

- Payment hubs skip TLS verify against hub OpenShift Routes (`ARBITRATOR_INSECURE_SKIP_VERIFY=true`) for lab certs.
- MM2 uses `DefaultReplicationPolicy`: mirrored topics appear as `cluster1-fis.payment-instructions` (etc.) on the peer.
- Strimzi install rewrites upstream `myproject` RoleBinding subjects to `kafka`.
