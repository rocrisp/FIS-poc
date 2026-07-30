#!/usr/bin/env bash
# Install the Strimzi Kafka operator.
# Run once against EACH cluster before applying any Kafka resources.
#
# Usage:
#   KUBECONFIG=~/.kube/cluster-a ./00-strimzi-install.sh
#   KUBECONFIG=~/.kube/cluster-b ./00-strimzi-install.sh

set -euo pipefail

# Prefer oc on OpenShift; fall back to kubectl.
if command -v oc >/dev/null 2>&1; then
  KUBECTL=(oc)
elif command -v kubectl >/dev/null 2>&1; then
  KUBECTL=(kubectl)
else
  echo "oc or kubectl required" >&2
  exit 1
fi

STRIMZI_VERSION="0.42.0"

echo "Creating kafka namespace..."
"${KUBECTL[@]}" create namespace kafka --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -

echo "Installing Strimzi ${STRIMZI_VERSION}..."
# Upstream YAML defaults RoleBinding subjects to namespace "myproject".
# Rewrite to "kafka" before apply so leader-election and namespaced RBAC work.
TMP_YAML="$(mktemp)"
curl -fsSL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
  | sed 's/namespace: myproject/namespace: kafka/g' > "$TMP_YAML"
"${KUBECTL[@]}" apply -f "$TMP_YAML" -n kafka
rm -f "$TMP_YAML"

# Strimzi 0.42.0 cannot infer newer Kubernetes versions (for example 1.35)
# unless STRIMZI_KUBERNETES_VERSION is set explicitly.
# Note: `oc version` does not support -o jsonpath; parse JSON instead.
VERSION_JSON=$("${KUBECTL[@]}" version -o json)
K8S_MAJOR=$(printf '%s' "$VERSION_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["major"])')
K8S_MINOR=$(printf '%s' "$VERSION_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["minor"].rstrip("+"))')
echo "Setting STRIMZI_KUBERNETES_VERSION=major=${K8S_MAJOR},minor=${K8S_MINOR}"
"${KUBECTL[@]}" set env deployment/strimzi-cluster-operator -n kafka \
  STRIMZI_KUBERNETES_VERSION="major=${K8S_MAJOR},minor=${K8S_MINOR}"

echo "Waiting for Strimzi operator to be ready..."
"${KUBECTL[@]}" rollout status deployment/strimzi-cluster-operator -n kafka --timeout=180s

echo "Strimzi operator ready."
