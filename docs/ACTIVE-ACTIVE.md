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
- marking a site unreachable fences only that site; the peer stays **sole active** (`writeMode=sole-active`, `soleActiveSite=<peer>`)

Overview fields:

| Field | Meaning |
|-------|---------|
| `fallbackActive` | Preferred writer if payment hubs lose the hub (`cluster1-fis`) |
| `soleActiveSite` | Only reachable site when the other is down |
| `writeMode` | `active-active` \| `sole-active` \| `none` |

**Cluster1 down:** hub reports `soleActiveSite=cluster2-fis` so cluster2 is clearly the active writer.

### Payment hub (`payment-hub-aa`)

1. Poll the AA arbitrator; refuse if `acceptTraffic=false` (fenced).
2. **Account affinity** when both sites are active:
   - first letter **a–m** → home `cluster1-fis`
   - first letter **n–z** → home `cluster2-fis`
3. **Sole active** (peer down): that site accepts **any** payer.
4. **Hub unreachable** (local policy — sites cannot ask the referee):
   - `cluster1-fis` becomes sole active (`hub_unreachable_fallback_active`)
   - `cluster2-fis` refuses (`hub_unreachable_cluster1_active`) until the hub returns
5. Kafka/MM2 + ledger UI same as the AP demo (separate consumer group).

Examples: `alice`/`bob` on cluster1; `nancy`/`oscar` on cluster2 (when both active).

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

## Failover / degrade policy

| Situation | Who accepts payments |
|-----------|----------------------|
| Both sites + hub healthy | Both (with letter affinity) |
| Cluster1 down (hub up) | **Cluster2 sole active** (any payer) — hub sets `soleActiveSite=cluster2-fis` |
| Cluster2 down (hub up) | **Cluster1 sole active** (any payer) |
| Hub unreachable from sites | **Cluster1 sole active**; cluster2 refuses until hub returns |
| GUI **Hub unreachable** (`hub-down`) | Same as above (demo without killing the route) |
| GUI **Submariner mesh down** (`partition`) | **Both stay active** — sync may lag; this is not hub-down |

## Demo walkthrough

1. Open both AA payment hubs — both show **ACTIVE** with `acceptTraffic=true`.
2. On cluster1 submit `alice → oscar`; both ledgers update via MM2.
3. On cluster2 submit `oscar → alice`; both ledgers update.
4. On cluster1 try `oscar → alice` → refused (`wrong_home_site`) while both active.
5. Hub AA GUI: **Mark cluster1 unreachable** → hub shows `writeMode=sole-active`, `soleActiveSite=cluster2-fis`; cluster2 accepts any payer.
6. **Clear (live signals)** → both active again.
7. (Optional) Block hub route from a site → cluster1 stays active; cluster2 refuses with `hub_unreachable_cluster1_active`.

## Why not stretched Akka Cluster?

Cross-site Akka Cluster active-active across distant OpenShift SNOs is fragile (gossip RTT, split-brain, membership). This PoC uses **two independent site processes**, hub fencing, async Kafka, and **payer affinity** for conflict avoidance.
