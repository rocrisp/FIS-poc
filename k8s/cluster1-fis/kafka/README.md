# Kafka active/standby sync (cluster1-fis ↔ cluster2-fis)

## Roles

| Site | Default ACM role | Kafka job |
|------|------------------|-----------|
| `cluster1-fis` | **active** (`acceptTraffic: true`) | Produces payment events; MM2 mirrors to cluster2 |
| `cluster2-fis` | **standby** (`acceptTraffic: false`) | Consumes mirrored events; stays warm for failover |

Standby means **healthy but not accepting new traffic** — not "down" or "bad".

## Deploy order

```bash
# Both sites: Strimzi + Kafka + topics
KUBECONFIG=~/Downloads/cluster1-fis-kubeconfig.yaml ./k8s/00-strimzi-install.sh
KUBECONFIG=~/Downloads/cluster2-fis-kubeconfig.yaml ./k8s/00-strimzi-install.sh

oc --kubeconfig ~/Downloads/cluster1-fis-kubeconfig.yaml apply -f k8s/cluster1-fis/kafka/01-kafka-cluster.yaml
oc --kubeconfig ~/Downloads/cluster2-fis-kubeconfig.yaml apply -f k8s/cluster2-fis/kafka/01-kafka-cluster.yaml

# Wait for external bootstrap, then topics + MM2 (fill placeholders first)
oc --kubeconfig ~/Downloads/cluster1-fis-kubeconfig.yaml apply -f k8s/cluster1-fis/kafka/02-topics.yaml
oc --kubeconfig ~/Downloads/cluster2-fis-kubeconfig.yaml apply -f k8s/cluster2-fis/kafka/02-topics.yaml
```

Topics: `payment-instructions` (inbound create/cancel) and `payment-lifecycle` (event history for rehydration).

Mirrored names on the target look like `cluster1-fis.payment-lifecycle` (MM2 default rename).
