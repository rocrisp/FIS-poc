# Active-active demo (parallel stack)

This repo keeps the original **active-passive** demo unchanged and adds a parallel **active-active** stack with distinct names, namespaces, and routes.

| Mode | Arbitrator | Payment app | Namespace |
|------|------------|-------------|-----------|
| **Active-passive** (original) | `akka-split-brain-arbitrator/` | `payment-hub-demo/` | `payment-hub` |
| **Active-active** (new) | `arbitrator-active-active/` | `payment-hub-active-active/` | `payment-hub-aa` |

Both stacks share the same Kafka/MM2 topology on `cluster1-fis` / `cluster2-fis`.

## Behavior

### Arbitrator (`fis-arbitrator-active-active`)

When both managed clusters are reachable:

- both sites get `role=active` and `acceptTraffic=true`
- mesh-down (`partition` simulation) keeps **both** active (`reason=active_mesh_degraded`) — sync may lag
- marking a site unreachable fences only that site; the peer stays active

### Payment hub (`payment-hub-aa`)

1. Poll the AA arbitrator; refuse if `acceptTraffic=false` (fenced).
2. **Account affinity** on the payer (`from`):
   - first letter **a–m** → home `cluster1-fis`
   - first letter **n–z** → home `cluster2-fis`
3. Wrong home → HTTP 409 `wrong_home_site` (even if the site is active).
4. Kafka/MM2 + ledger UI same as the AP demo (separate consumer group).

Examples: `alice`/`bob` on cluster1; `nancy`/`oscar` on cluster2.

## Deploy

```bash
export HUB_KUBECONFIG=~/Downloads/rose-fis-kubeconfig.yaml
export CLUSTER1_KUBECONFIG=~/Downloads/cluster1-fis-kubeconfig.yaml
export CLUSTER2_KUBECONFIG=~/Downloads/cluster2-fis-kubeconfig.yaml

# Kafka/MM2 already from the AP demo — reuse
./scripts/deploy-arbitrator-aa.sh
export ARBITRATOR_AA_URL="https://$(oc --kubeconfig "$HUB_KUBECONFIG" -n open-cluster-management get route fis-arbitrator-active-active -o jsonpath='{.spec.host}')"

ARBITRATOR_URL="$ARBITRATOR_AA_URL" ./scripts/deploy-payment-hub-aa.sh cluster1-fis
ARBITRATOR_URL="$ARBITRATOR_AA_URL" ./scripts/deploy-payment-hub-aa.sh cluster2-fis
```

## Lab URLs (after deploy)

| Component | Expected host pattern |
|-----------|------------------------|
| AA arbitrator | `fis-arbitrator-active-active-open-cluster-management.apps.rose-fis.opdev.io` |
| AA hub cluster1 | `payment-hub-aa-payment-hub-aa.apps.cluster1-fis.opdev.io` |
| AA hub cluster2 | `payment-hub-aa-payment-hub-aa.apps.cluster2-fis.opdev.io` |

## Demo walkthrough

1. Open both AA payment hubs — both show **ACTIVE** with `acceptTraffic=true`.
2. On cluster1 submit `alice → oscar`; both ledgers update via MM2.
3. On cluster2 submit `oscar → alice`; both ledgers update.
4. On cluster1 try `oscar → alice` → refused (`wrong_home_site`).
5. Hub AA GUI: **Mark cluster1 unreachable** → cluster1 fenced; cluster2 still active for its home accounts.
6. **Clear (live signals)** → both active again.

## Why not stretched Akka Cluster?

Cross-site Akka Cluster active-active across distant OpenShift SNOs is fragile (gossip RTT, split-brain, membership). This PoC uses **two independent site processes**, hub fencing, async Kafka, and **payer affinity** for conflict avoidance.
