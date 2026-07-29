# Kubernetes rolling and scale acceptance

`run-orbstack.sh` uses the active OrbStack Kubernetes context. It creates an
isolated namespace with ephemeral PostgreSQL and MinIO dependencies, installs
the production Helm chart with three stateless Agents, and runs a Kafka 4.2
client inside the cluster.

The gate writes continuously while changing the Agent image, requires every
original pod to be replaced, scales from three Agents to five and back to two,
and verifies one contiguous, duplicate-free partition after every transition.
It also checks Helm lint/template output, migration state, readiness,
PodDisruptionBudget configuration, automatic Service advertisement, and the
absence of Agent PVCs or data volumes.

Run with the Rust 1.88 release binary already built:

```bash
RUTOMQ_K8S_PREBUILT=1 tests/kubernetes/run-orbstack.sh
```

The namespace is deleted on exit unless `KEEP_CLUSTER=1` is set.
