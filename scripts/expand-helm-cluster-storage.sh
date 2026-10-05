#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: expand-helm-cluster-storage.sh <release> <namespace> <new-size> [chart]

Expands every PVC in the fixed three-member Cursus chart, then safely
recreates the immutable StatefulSet claim template through Helm. Run this as
a storage-only release operation before applying image or configuration changes.
EOF
}

if [[ $# -lt 3 || $# -gt 4 ]]; then
  usage >&2
  exit 2
fi

release=$1
namespace=$2
target_size=$3
chart=${4:-manifests/helm-cluster}
timeout=${CURSUS_STORAGE_EXPANSION_TIMEOUT:-10m}

for command in kubectl helm; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

statefulset=$(kubectl -n "$namespace" get statefulset \
  -l "app.kubernetes.io/instance=$release" \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
if [[ -z "$statefulset" || "$statefulset" == *$'\n'* ]]; then
  echo "expected exactly one StatefulSet for release $release in namespace $namespace" >&2
  exit 1
fi

current_size=$(kubectl -n "$namespace" get statefulset "$statefulset" \
  -o jsonpath='{.spec.volumeClaimTemplates[0].spec.resources.requests.storage}')
if [[ "$current_size" == "$target_size" ]]; then
  echo "storage is already configured at $target_size"
  exit 0
fi

pvcs=()
for ordinal in 0 1 2; do
  pvc="logs-${statefulset}-${ordinal}"
  kubectl -n "$namespace" get pvc "$pvc" >/dev/null
  storage_class=$(kubectl -n "$namespace" get pvc "$pvc" -o jsonpath='{.spec.storageClassName}')
  if [[ -z "$storage_class" ]]; then
    echo "PVC $pvc has no resolved StorageClass" >&2
    exit 1
  fi
  if [[ $(kubectl get storageclass "$storage_class" -o jsonpath='{.allowVolumeExpansion}') != "true" ]]; then
    echo "StorageClass $storage_class for PVC $pvc does not allow volume expansion" >&2
    exit 1
  fi
  # Kubernetes performs quantity-aware validation here and rejects shrink.
  kubectl -n "$namespace" patch pvc "$pvc" --type merge \
    -p "{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"$target_size\"}}}}" \
    --dry-run=server >/dev/null
  pvcs+=("$pvc")
done

for pvc in "${pvcs[@]}"; do
  kubectl -n "$namespace" patch pvc "$pvc" --type merge \
    -p "{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"$target_size\"}}}}" >/dev/null
done

ready_count() {
  kubectl -n "$namespace" get pods -l "app.kubernetes.io/instance=$release" \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' | grep -c '^true$' || true
}

for ordinal in 0 1 2; do
  pvc=${pvcs[$ordinal]}
  capacity=$(kubectl -n "$namespace" get pvc "$pvc" -o jsonpath='{.status.capacity.storage}')
  if [[ "$capacity" == "$target_size" ]]; then
    continue
  fi
  if (( $(ready_count) < 2 )); then
    echo "refusing to restart $statefulset-$ordinal without two Ready peers" >&2
    exit 1
  fi
  kubectl -n "$namespace" delete pod "$statefulset-$ordinal" --wait=false
  kubectl -n "$namespace" wait --for=condition=Ready "pod/$statefulset-$ordinal" --timeout="$timeout"
  if (( $(ready_count) != 3 )); then
    echo "cluster did not return to three Ready members after restarting ordinal $ordinal" >&2
    exit 1
  fi
done

for pvc in "${pvcs[@]}"; do
  requested=$(kubectl -n "$namespace" get pvc "$pvc" -o jsonpath='{.spec.resources.requests.storage}')
  capacity=$(kubectl -n "$namespace" get pvc "$pvc" -o jsonpath='{.status.capacity.storage}')
  if [[ "$requested" != "$target_size" || "$capacity" != "$target_size" ]]; then
    echo "PVC $pvc has request=$requested capacity=$capacity; expected $target_size" >&2
    exit 1
  fi
done

# volumeClaimTemplates is immutable. Orphaning preserves all Pods and PVCs;
# Helm immediately recreates only the controller with the larger template.
kubectl -n "$namespace" delete statefulset "$statefulset" --cascade=orphan
helm upgrade "$release" "$chart" --namespace "$namespace" --reuse-values \
  --set "persistence.size=$target_size" --atomic --wait --timeout "$timeout"

installed_size=$(kubectl -n "$namespace" get statefulset "$statefulset" \
  -o jsonpath='{.spec.volumeClaimTemplates[0].spec.resources.requests.storage}')
if [[ "$installed_size" != "$target_size" || $(ready_count) != 3 ]]; then
  echo "storage expansion completed incompletely: template=$installed_size ready=$(ready_count)" >&2
  exit 1
fi

echo "expanded $release storage from $current_size to $target_size on all three members"
