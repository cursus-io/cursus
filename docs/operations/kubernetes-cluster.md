# Three-member Kubernetes cluster

`manifests/helm-cluster` is a deliberately fixed three-member production topology. It is separate from the existing standalone chart so that a values change cannot turn a single PVC into a false cluster deployment.

Each StatefulSet ordinal owns one `ReadWriteOnce` PVC, publishes a stable headless-service DNS name, and uses those three names for Raft membership and discovery. Initial Pods are created in parallel because the Raft bootstrap needs all three voters; broker `-0` is the only member permitted to bootstrap an empty cluster. Restart recovery uses the existing Raft state on each PVC. The PodDisruptionBudget requires two available members and the default required anti-affinity therefore requires three schedulable nodes.

The chart supports in-cluster clients only. Brokers advertise their stable Pod
DNS names in metadata, so the chart rejects `NodePort` and `LoadBalancer`
services rather than returning unreachable addresses to external clients. Use
an in-cluster application, a port-forward for administration, or a gateway
that understands broker metadata until an external advertised-address
topology is implemented.

The default Pod and container security contexts satisfy Kubernetes restricted Pod Security: the broker runs as UID 1000 without privilege escalation or Linux capabilities, uses a read-only root filesystem, and inherits a `RuntimeDefault` seccomp profile from the Pod. A platform that requires an approved local profile may override `podSecurityContext.seccompProfile` with `type: Localhost` and `localhostProfile`; validate that profile on every node before installation.

Each broker requests `500m` CPU and `1Gi` of memory so the scheduler accounts
for a usable baseline. The chart deliberately leaves CPU and memory limits to
the operator: a generic memory limit can turn load spikes into broker OOM
restarts. Set limits only after measuring peak live data, page cache, recovery,
and compaction on the target workload. Keep enough allocatable capacity for
all three requested Pods on separate nodes.

## Install

Resolve the immutable digest and 40-character source revision for one published
image. They must refer to the same build. The chart has no runnable default
image and rejects mutable tags in production:

```bash
IMAGE=ghcr.io/cursus-io/cursus:VERSION
DIGEST=$(docker buildx imagetools inspect "$IMAGE" --format '{{json .Manifest.Digest}}' | tr -d '"')
REVISION=$(docker buildx imagetools inspect "$IMAGE" --format '{{json .Image.Config.Labels."org.opencontainers.image.revision"}}' | tr -d '"')
test "${#REVISION}" -eq 40
```

Create the namespace first, then the shared internal-authentication Secret and a TLS Secret whose certificate is valid for `cursus.internal` and the three generated pod DNS names. Do not use the client TLS Secret for this purpose unless it carries the required client-auth CA and SANs.

```bash
kubectl create namespace brokers

kubectl -n brokers create secret generic cursus-internal-auth \
  --from-literal=token="$(openssl rand -hex 32)"

kubectl -n brokers create secret generic cursus-internal-tls \
  --from-file=tls.crt=broker.crt \
  --from-file=tls.key=broker.key \
  --from-file=ca.crt=ca.crt

helm upgrade --install cursus manifests/helm-cluster --namespace brokers --create-namespace \
  --set-string image.digest="$DIGEST" \
  --set-string image.revision="$REVISION" \
  --set cluster.internalAuthSecret=cursus-internal-auth \
  --set-string cluster.internalAuthGeneration=1 \
  --set internalTLS.secretName=cursus-internal-tls
```

Before production use, render and inspect the exact release:

```bash
helm lint manifests/helm-cluster \
  --set-string image.digest="$DIGEST" \
  --set-string image.revision="$REVISION" \
  --set cluster.internalAuthSecret=cursus-internal-auth \
  --set internalTLS.secretName=cursus-internal-tls
helm template cursus manifests/helm-cluster --namespace brokers \
  --set-string image.digest="$DIGEST" \
  --set-string image.revision="$REVISION" \
  --set cluster.internalAuthSecret=cursus-internal-auth \
  --set internalTLS.secretName=cursus-internal-tls | kubectl apply --dry-run=client -f -
kubectl -n brokers wait --for=condition=Ready pod \
  -l app.kubernetes.io/instance=cursus --timeout=5m
kubectl -n brokers get pods,pvc,pdb
kubectl -n brokers get events --field-selector reason=FailedCreate
```

## Restart, recovery, and upgrades

Do not delete PVCs while restarting, scaling, or upgrading the StatefulSet. Replace one Pod at a time and wait for it to become ready before touching the next member; a concurrent two-member outage removes the configured write quorum. `updateStrategy: OnDelete` makes that operator action explicit.

`persistence.size` is install-only during a normal Helm upgrade because Kubernetes makes StatefulSet `volumeClaimTemplates` immutable. The chart rejects a changed value before submitting an invalid StatefulSet update. Expand storage as its own release operation before combining it with an image or configuration upgrade:

```bash
scripts/expand-helm-cluster-storage.sh cursus brokers 30Gi
helm upgrade cursus manifests/helm-cluster --namespace brokers --reuse-values \
  --set-string image.digest="$NEW_DIGEST" \
  --set-string image.revision="$NEW_REVISION"
```

When Prometheus Operator CRDs are installed, enable the metrics Service,
ServiceMonitor, and baseline alerts with `monitoring.enabled=true`. Set
`monitoring.labels` to the labels selected by the installed operator, and tune
the lag and transaction-age thresholds before routing alerts to responders.

The expansion command validates all three PVCs and their StorageClasses before mutation. Kubernetes server-side validation rejects a shrink. It patches every claim, performs one-member-at-a-time restarts only when the CSI driver requires filesystem expansion, waits for all three members between restarts, verifies requested and filesystem capacity, orphans the running Pods and claims, and uses an atomic Helm upgrade to recreate the StatefulSet with the new claim template. Do not edit `persistence.size` directly or combine unrelated release changes into the expansion command. Keep a current backup and verify a known acknowledged payload and committed consumer offset before and after this procedure.

Every Pod first runs the selected image as an init container. The binary must
report the exact image revision, Wire protocol, broker lifecycle protocol,
Raft snapshot format, and disk record format required by this chart. A stale
image, a digest/revision mismatch, or an unsupported storage/protocol contract
prevents the broker container from starting.

## Rotate internal credentials without losing quorum

The token Secret may contain `token` (the active outbound credential) and `next-token` (an additional inbound credential). `cluster.internalAuthGeneration` is a non-secret label surfaced on Pods and by `CLUSTER_STATUS`. Rotate in three complete one-Pod-at-a-time passes, waiting for all three Pods to be Ready and checking `CLUSTER_STATUS` after every Pod:

1. Add the new value as `next-token` while retaining the old `token`, increment the generation label to an overlap value, and restart all three Pods one at a time. Every broker still sends the old token and now accepts both.
2. Exchange the Secret values so `token` is new and `next-token` is old, set the new active generation, and restart all three Pods one at a time. New senders remain accepted by peers from the first pass.
3. Remove `next-token` and restart all three Pods one at a time. Verify that an internal request signed with the old token fails after the final Pod is Ready.

If the active token may already be compromised, do not wait between passes for routine scheduling windows: keep client writes observed, execute the same overlap sequence immediately, and revoke the old token in the third pass. Skipping the overlap pass can split internal traffic and remove write quorum.

Internal mTLS rotates with the same trust sequence. First publish a `ca.crt` bundle containing both old and new CA certificates while keeping the old leaf certificate, then restart all three Pods one at a time. Next publish the new `tls.crt` and `tls.key` while retaining both CAs and repeat the rolling restart. Finally remove the old CA and repeat the rolling restart. Certificates must keep the documented StatefulSet DNS SANs and client-auth usage. After the final pass, verify the old client certificate and old token are both rejected while continuous `acks=all` writes, committed offsets, and a known payload remain intact.

For an irrecoverable node, stop client writes, preserve the failed PVC for forensics, restore that member from the same backup generation as the other members, and then recreate only that Pod. If the Raft membership itself is damaged, stop and follow the coordinated backup/restore procedure in `upgrade-and-recovery.md`; replacing storage from different backup generations is not supported.

The chart guarantees rendered topology and safety constraints, not a completed live cluster qualification. Run the cluster E2E suite and the documented restart, Pod-recreation, quorum-loss, and restore drills on the target Kubernetes distribution before accepting traffic.
