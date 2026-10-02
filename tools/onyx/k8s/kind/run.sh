#!/usr/bin/env bash
# Deploys user-service's generated Kubernetes base to a local kind cluster. Run from the repo root.
# The cluster gets its own kubeconfig, so the current kubectl context is left alone.
set -euo pipefail

CLUSTER=onyx
DIR=tools/onyx/k8s/kind
IMAGE=onyx-kind/user-service:dev
export KUBECONFIG="$PWD/plz-out/kind-$CLUSTER.kubeconfig"

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER"
fi
kind export kubeconfig --name "$CLUSTER" >/dev/null

plz build //cmd/user-service //cmd/user-service:k8s -p >/dev/null
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
cp plz-out/bin/cmd/user-service/user-service "$context/"
docker build -q -t "$IMAGE" -f "$DIR/Dockerfile" "$context" >/dev/null
kind load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null

kubectl apply -k "$DIR"
# The tag never changes, so restart to pick up a rebuilt image.
kubectl -n onyx rollout restart deployment/user-service >/dev/null
for d in postgres nats user-service; do
  kubectl -n onyx rollout status "deployment/$d" --timeout=180s
done

echo
echo "export KUBECONFIG=$KUBECONFIG"
