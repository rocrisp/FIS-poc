#!/usr/bin/env bash
# Build and deploy the active-active arbitrator on the hub (rose-fis).
# Parallel to deploy-arbitrator.sh (active-passive); does not replace it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HUB_KUBECONFIG="${HUB_KUBECONFIG:-$HOME/Downloads/rose-fis-kubeconfig.yaml}"
NS=open-cluster-management
APP_DIR="$ROOT/arbitrator-active-active"
NAME=fis-arbitrator-active-active

export KUBECONFIG="$HUB_KUBECONFIG"

echo "==> Applying RBAC + config + service + route (active-active)"
oc apply -f "$APP_DIR/k8s/rbac.yaml"
oc apply -f "$APP_DIR/k8s/config.yaml"
oc apply -f "$APP_DIR/k8s/service.yaml"
oc apply -f "$APP_DIR/k8s/route.yaml"

if ! oc -n "$NS" get buildconfig "$NAME" >/dev/null 2>&1; then
  echo "==> Creating binary BuildConfig + ImageStream"
  oc -n "$NS" new-build --binary --strategy=docker --name="$NAME"
fi

echo "==> Starting binary build from $APP_DIR"
(
  cd "$APP_DIR"
  go mod tidy
)
oc -n "$NS" start-build "$NAME" --from-dir="$APP_DIR" --follow

echo "==> Deploying"
oc apply -f "$APP_DIR/k8s/deployment.yaml"
oc -n "$NS" set image deployment/"$NAME" \
  arbitrator="$NAME:latest" --source=imagestreamtag
oc -n "$NS" rollout restart deployment/"$NAME"
oc -n "$NS" rollout status deployment/"$NAME" --timeout=180s
DIGEST="$(oc -n "$NS" get istag "$NAME:latest" -o jsonpath='{.image.dockerImageReference}')"
if [[ -n "$DIGEST" ]]; then
  oc -n "$NS" set image deployment/"$NAME" arbitrator="$DIGEST"
  oc -n "$NS" rollout status deployment/"$NAME" --timeout=180s
fi

ROUTE="$(oc -n "$NS" get route "$NAME" -o jsonpath='{.spec.host}')"
echo "==> Active-active arbitrator route: https://$ROUTE"
echo "    curl -sk https://$ROUTE/api/v1/overview | jq ."
echo "    curl -sk -H 'X-Cluster-Name: cluster1-fis' https://$ROUTE/api/v1/state | jq '.datacenter'"
echo "    curl -sk -H 'X-Cluster-Name: cluster2-fis' https://$ROUTE/api/v1/state | jq '.datacenter'"
