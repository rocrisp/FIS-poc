#!/usr/bin/env bash
# Deploy SNO Kafka (+ optional MM2) to one FIS managed cluster.
# Usage:
#   ./scripts/deploy-kafka-site.sh cluster1-fis
#   ./scripts/deploy-kafka-site.sh cluster2-fis
# After both Kafka clusters are Ready, fill MM2 bootstrap placeholders and re-apply 03-mirrormaker2.yaml
set -euo pipefail

SITE="${1:?site name required: cluster1-fis|cluster2-fis}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
case "$SITE" in
  cluster1-fis) KC="${CLUSTER1_KUBECONFIG:-$HOME/Downloads/cluster1-fis-kubeconfig.yaml}" ;;
  cluster2-fis) KC="${CLUSTER2_KUBECONFIG:-$HOME/Downloads/cluster2-fis-kubeconfig.yaml}" ;;
  *) echo "unknown site: $SITE"; exit 1 ;;
esac

export KUBECONFIG="$KC"
DIR="$ROOT/k8s/$SITE/kafka"

echo "==> Strimzi on $SITE"
"$ROOT/k8s/00-strimzi-install.sh"

echo "==> Kafka cluster"
oc apply -f "$DIR/01-kafka-cluster.yaml"
echo "Waiting for Kafka to be Ready..."
oc -n kafka wait kafka/kafka-cluster --for=condition=Ready --timeout=600s || true

echo "==> Topics"
oc apply -f "$DIR/02-topics.yaml"

BOOT="$(oc -n kafka get kafka kafka-cluster -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}' 2>/dev/null || true)"
echo "==> External bootstrap for peer MM2: ${BOOT:-<pending>}"
echo "When both sites have bootstrap addresses, edit 03-mirrormaker2.yaml placeholders and:"
echo "  oc --kubeconfig \$KC apply -f $DIR/03-mirrormaker2.yaml"
