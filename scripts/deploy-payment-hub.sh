#!/usr/bin/env bash
# Deploy payment-hub-demo to a managed cluster.
# Usage:
#   ARBITRATOR_URL=https://... ./scripts/deploy-payment-hub.sh cluster1-fis
set -euo pipefail

SITE="${1:?site name required: cluster1-fis|cluster2-fis}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
case "$SITE" in
  cluster1-fis) KC="${CLUSTER1_KUBECONFIG:-$HOME/Downloads/cluster1-fis-kubeconfig.yaml}" ;;
  cluster2-fis) KC="${CLUSTER2_KUBECONFIG:-$HOME/Downloads/cluster2-fis-kubeconfig.yaml}" ;;
  *) echo "unknown site: $SITE"; exit 1 ;;
esac

export KUBECONFIG="$KC"
NS=payment-hub
APP_DIR="$ROOT/payment-hub-demo"
ARBITRATOR_URL="${ARBITRATOR_URL:?set ARBITRATOR_URL to the hub Route URL}"

oc apply -f "$APP_DIR/k8s/00-namespace.yaml"

if ! oc -n "$NS" get buildconfig payment-hub-demo >/dev/null 2>&1; then
  echo "==> Creating binary BuildConfig + ImageStream"
  oc -n "$NS" new-build --binary --strategy=docker --name=payment-hub-demo
fi

echo "==> Building payment-hub-demo on $SITE"
(
  cd "$APP_DIR"
  go mod tidy
)
oc -n "$NS" start-build payment-hub-demo --from-dir="$APP_DIR" --follow

case "$SITE" in
  cluster1-fis) PEER=cluster2-fis ;;
  cluster2-fis) PEER=cluster1-fis ;;
esac

# Render deployment with site name + peer + arbitrator URL
TMP="$(mktemp)"
sed -e "s/CLUSTER_NAME_PLACEHOLDER/$SITE/g" \
    -e "s/PEER_CLUSTER_PLACEHOLDER/$PEER/g" \
    -e "s|https://akka-split-brain-arbitrator-open-cluster-management.apps.rose-fis.opdev.io|$ARBITRATOR_URL|g" \
    "$APP_DIR/k8s/01-deployment.yaml" > "$TMP"
oc apply -f "$TMP"
rm -f "$TMP"

oc -n "$NS" set image deployment/payment-hub \
  payment-hub=payment-hub-demo:latest --source=imagestreamtag
DIGEST="$(oc -n "$NS" get istag payment-hub-demo:latest -o jsonpath='{.image.dockerImageReference}' 2>/dev/null || true)"
if [[ -n "$DIGEST" ]]; then
  oc -n "$NS" set image deployment/payment-hub payment-hub="$DIGEST"
fi
oc -n "$NS" rollout status deployment/payment-hub --timeout=180s

ROUTE="$(oc -n "$NS" get route payment-hub -o jsonpath='{.spec.host}')"
echo "==> payment-hub on $SITE: https://$ROUTE"
echo "    curl -s https://$ROUTE/health | jq ."
echo "    curl -s -X POST https://$ROUTE/api/v1/payments -H 'Content-Type: application/json' \\"
echo "      -d '{\"amount\":100,\"from\":\"alice\",\"to\":\"bob\"}' | jq ."
