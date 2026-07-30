#!/usr/bin/env bash
# Deploy payment-hub-active-active to a managed cluster.
# Parallel to deploy-payment-hub.sh (active-passive); uses namespace payment-hub-aa.
# Usage:
#   ARBITRATOR_URL=https://... ./scripts/deploy-payment-hub-aa.sh cluster1-fis
set -euo pipefail

SITE="${1:?site name required: cluster1-fis|cluster2-fis}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
case "$SITE" in
  cluster1-fis) KC="${CLUSTER1_KUBECONFIG:-$HOME/Downloads/cluster1-fis-kubeconfig.yaml}" ;;
  cluster2-fis) KC="${CLUSTER2_KUBECONFIG:-$HOME/Downloads/cluster2-fis-kubeconfig.yaml}" ;;
  *) echo "unknown site: $SITE"; exit 1 ;;
esac

export KUBECONFIG="$KC"
NS=payment-hub-aa
APP_DIR="$ROOT/payment-hub-active-active"
NAME=payment-hub-aa
ARBITRATOR_URL="${ARBITRATOR_URL:?set ARBITRATOR_URL to the active-active hub Route URL}"

oc apply -f "$APP_DIR/k8s/00-namespace.yaml"

if ! oc -n "$NS" get buildconfig "$NAME" >/dev/null 2>&1; then
  echo "==> Creating binary BuildConfig + ImageStream"
  oc -n "$NS" new-build --binary --strategy=docker --name="$NAME"
fi

echo "==> Building $NAME on $SITE"
(
  cd "$APP_DIR"
  go mod tidy
)
oc -n "$NS" start-build "$NAME" --from-dir="$APP_DIR" --follow

case "$SITE" in
  cluster1-fis) PEER=cluster2-fis ;;
  cluster2-fis) PEER=cluster1-fis ;;
esac

TMP="$(mktemp)"
sed -e "s/CLUSTER_NAME_PLACEHOLDER/$SITE/g" \
    -e "s/PEER_CLUSTER_PLACEHOLDER/$PEER/g" \
    -e "s|https://fis-arbitrator-active-active-open-cluster-management.apps.rose-fis.opdev.io|$ARBITRATOR_URL|g" \
    "$APP_DIR/k8s/01-deployment.yaml" > "$TMP"
oc apply -f "$TMP"
rm -f "$TMP"

oc -n "$NS" set image deployment/"$NAME" \
  payment-hub-aa="$NAME:latest" --source=imagestreamtag
DIGEST="$(oc -n "$NS" get istag "$NAME:latest" -o jsonpath='{.image.dockerImageReference}' 2>/dev/null || true)"
if [[ -n "$DIGEST" ]]; then
  oc -n "$NS" set image deployment/"$NAME" payment-hub-aa="$DIGEST"
fi
oc -n "$NS" rollout status deployment/"$NAME" --timeout=180s

ROUTE="$(oc -n "$NS" get route "$NAME" -o jsonpath='{.spec.host}')"
echo "==> payment-hub-aa on $SITE: https://$ROUTE"
echo "    curl -s https://$ROUTE/health | jq ."
echo "    # cluster1 home (alice); cluster2 home (oscar)"
echo "    curl -s -X POST https://$ROUTE/api/v1/payments -H 'Content-Type: application/json' \\"
if [[ "$SITE" == "cluster1-fis" ]]; then
  echo "      -d '{\"amount\":100,\"from\":\"alice\",\"to\":\"oscar\"}' | jq ."
else
  echo "      -d '{\"amount\":100,\"from\":\"oscar\",\"to\":\"alice\"}' | jq ."
fi
