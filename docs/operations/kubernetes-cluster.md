# Three-member Kubernetes cluster

`manifests/helm-cluster` is a deliberately fixed three-member production topology. It is separate from the existing standalone chart so that a values change cannot turn a single PVC into a false cluster deployment.

Each StatefulSet ordinal owns one `ReadWriteOnce` PVC, publishes a stable headless-service DNS name, and uses those three names for Raft membership and discovery. Initial Pods are created in parallel because the Raft bootstrap needs all three voters; broker `-0` is the only member permitted to bootstrap an empty cluster. Restart recovery uses the existing Raft state on each PVC. The PodDisruptionBudget requires two available members and the default required anti-affinity therefore requires three schedulable nodes.

## Install

Create the shared internal-authentication Secret and a TLS Secret whose certificate is valid for `cursus.internal` and the three generated pod DNS names. Do not use the client TLS Secret for this purpose unless it carries the required client-auth CA and SANs.

```bash
kubectl -n brokers create secret generic cursus-internal-auth \
  --from-literal=token="$(openssl rand -hex 32)"

kubectl -n brokers create secret generic cursus-internal-tls \
  --from-file=tls.crt=broker.crt \
  --from-file=tls.key=broker.key \
  --from-file=ca.crt=ca.crt

helm upgrade --install cursus manifests/helm-cluster --namespace brokers --create-namespace \
  --set cluster.internalAuthSecret=cursus-internal-auth \
  --set internalTLS.secretName=cursus-internal-tls
```

Before production use, render and inspect the exact release:

```bash
helm lint manifests/helm-cluster \
  --set cluster.internalAuthSecret=cursus-internal-auth \
  --set internalTLS.secretName=cursus-internal-tls
helm template cursus manifests/helm-cluster --namespace brokers \
  --set cluster.internalAuthSecret=cursus-internal-auth \
  --set internalTLS.secretName=cursus-internal-tls | kubectl apply --dry-run=client -f -
kubectl -n brokers rollout status statefulset/cursus-cursus-cluster
kubectl -n brokers get pods,pvc,pdb
```

## Restart, recovery, and upgrades

Do not delete PVCs while restarting, scaling, or upgrading the StatefulSet. Replace one Pod at a time and wait for it to become ready before touching the next member; a concurrent two-member outage removes the configured write quorum. `updateStrategy: OnDelete` makes that operator action explicit.

For an irrecoverable node, stop client writes, preserve the failed PVC for forensics, restore that member from the same backup generation as the other members, and then recreate only that Pod. If the Raft membership itself is damaged, stop and follow the coordinated backup/restore procedure in `upgrade-and-recovery.md`; replacing storage from different backup generations is not supported.

The chart guarantees rendered topology and safety constraints, not a completed live cluster qualification. Run the cluster E2E suite and the documented restart, Pod-recreation, quorum-loss, and restore drills on the target Kubernetes distribution before accepting traffic.
