#!/usr/bin/env bash
# Fill MM2 bootstrap placeholders from live Kafka external listeners and apply.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
C1_KC="${CLUSTER1_KUBECONFIG:-$HOME/Downloads/cluster1-fis-kubeconfig.yaml}"
C2_KC="${CLUSTER2_KUBECONFIG:-$HOME/Downloads/cluster2-fis-kubeconfig.yaml}"

BOOT1="$(KUBECONFIG="$C1_KC" oc -n kafka get kafka kafka-cluster -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}')"
BOOT2="$(KUBECONFIG="$C2_KC" oc -n kafka get kafka kafka-cluster -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}')"

if [[ -z "$BOOT1" || -z "$BOOT2" ]]; then
  echo "external bootstrap missing: cluster1='$BOOT1' cluster2='$BOOT2'" >&2
  exit 1
fi

echo "cluster1-fis external: $BOOT1"
echo "cluster2-fis external: $BOOT2"

TMP1="$(mktemp)"; TMP2="$(mktemp)"
sed "s|CLUSTER2_FIS_EXTERNAL_BOOTSTRAP|$BOOT2|g" \
  "$ROOT/k8s/cluster1-fis/kafka/03-mirrormaker2.yaml" > "$TMP1"
sed "s|CLUSTER1_FIS_EXTERNAL_BOOTSTRAP|$BOOT1|g" \
  "$ROOT/k8s/cluster2-fis/kafka/03-mirrormaker2.yaml" > "$TMP2"

KUBECONFIG="$C1_KC" oc apply -f "$TMP1"
KUBECONFIG="$C2_KC" oc apply -f "$TMP2"
rm -f "$TMP1" "$TMP2"

echo "Waiting for MirrorMaker2 Ready (up to 3m each)..."
KUBECONFIG="$C1_KC" oc -n kafka wait kafkamirrormaker2/mm2-to-cluster2-fis --for=condition=Ready --timeout=180s || true
KUBECONFIG="$C2_KC" oc -n kafka wait kafkamirrormaker2/mm2-to-cluster1-fis --for=condition=Ready --timeout=180s || true
KUBECONFIG="$C1_KC" oc -n kafka get kafkamirrormaker2,pods -l strimzi.io/kind=KafkaMirrorMaker2
KUBECONFIG="$C2_KC" oc -n kafka get kafkamirrormaker2,pods -l strimzi.io/kind=KafkaMirrorMaker2
