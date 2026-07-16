# Design And Contribution Principles

Cursus favors a compact implementation, but simplicity must not weaken a documented reliability or security guarantee.

1. **Partition-local authority**: ordering, offsets, producer sequence state, high watermarks, and transaction visibility must have one explicit owner.
2. **Durability before visibility**: acknowledgements and `read_committed` visibility must follow the configured flush, replication, and coordinator decision contracts.
3. **Separated control and data paths**: group and transaction coordinators own fenced metadata; partition leaders own records; internal replication commands stay behind the broker security boundary.
4. **Monotonic state**: offsets, generations, producer epochs, stream versions, and committed tails must not move backward.
5. **Recoverable state**: every authoritative in-memory index needs a durable source and a tested rebuild or snapshot path.
6. **Structured protocol behavior**: successes start with `OK` or use documented binary/JSON frames; failures use `ERROR: <code>` and expose retry/fencing semantics.
7. **Bounded compatibility**: add version/feature negotiation for contract changes and keep SDK behavior aligned across Go, Java, and Python.
8. **Operational evidence**: shared-path changes require unit tests, integration coverage, and failure-window tests proportional to their blast radius.

## Change Checklist

- Start from current upstream `main` and keep unrelated worktree changes intact.
- Update `docs/protocol-spec.md` and `docs/reference/api-reference.md` for wire-visible behavior.
- Update the in-repository Go SDK when the broker contract changes; raise explicit follow-up work for external SDK repositories.
- Add restart, retry, stale-owner, and partial-failure coverage for coordinator or storage changes.
- Run formatting, focused tests, `go test ./...`, static checks used by CI, and `git diff --check` before requesting review.
- Do not describe an aspirational feature as implemented. State unsupported policies and external-system boundaries explicitly.

Standalone mode and clustered mode share the same client contract. Cluster features add coordinator routing, Raft-backed metadata, leader ownership, quorum replication, and internal transport security; they are not a separate protocol product.
