# EventFS Context

## Materialized path mapping

Definition: The rules that translate between native JetStream storage paths and human-facing materialized paths. The implementation lives in `eventfs-protocol::mount_paths`.

Important invariants:
- Native KV paths under `/kv/<bucket>/<key>` can have materialized aliases under `/tasks`, `/agents`, and `/semantic`.
- Stream subjects can have materialized aliases under `/events` and agent mailboxes.
- The same mapping drives queue visibility, local mutation invalidation, and watch invalidation.
- Affected paths carry a reason, not just a path string, so callers can distinguish exact paths, ancestors, aliases, and mailboxes without re-deriving mapping rules.
- The core interface accepts storage facts such as KV bucket/key, stream/subject, and object bucket/name. Queue operations, watch subjects, and materialized targets adapt into those storage facts instead of becoming part of the core interface.
- Visible paths and invalidation paths are separate outputs. Visible paths include exact paths and aliases that represent mount-visible files; invalidation paths additionally include ancestors that must be reclassified after mutation or watch events.
- Affected paths use a `MountPath` value rather than raw strings. `MountPath` owns normalization invariants: leading slash, no redundant slash, no trailing slash except root.
- `MountPath` is constructible from arbitrary path input through validation and normalization, with internal helpers for known-good joins. It is not limited to materialized path mapping outputs.

Related terms: JetStream path, materialized target, affected path, mount path, writeback queue, watch invalidation.

## Mount projection

Definition: The module seam that turns parsed mount paths, durable storage facts, pending write facts, cache facts, and invalidation facts into mount-visible outcomes.

Important invariants:
- Mount projection owns POSIX-facing decisions: path kind, file attributes, read bytes, directory entries, queued overlay visibility, and mutation outcome classification.
- FUSE kernel request and response types remain outside the seam. FUSE callbacks adapt kernel inputs into mount projection inputs and adapt outcomes back to kernel responses.
- Concrete JetStream calls, NATS subject parsing, and durable queue file IO remain outside the seam.
- The interface is deep only if callers no longer need to know alias expansion, queue overlay conflict rules, cache invalidation details, or replay state internals.

Related terms: materialized path mapping, mount path, writeback replay, invalidation plan, JetStream adapter.

## Queued overlay projection

Definition: The module seam that turns durable pending-write facts into mount-visible overlay facts before mount runtime and mount projection classify paths, directory entries, read bytes, attributes, and create targets.

Important invariants:
- Queued overlay projection owns pending write visibility, queued whole-value snapshots, queued JSONL append visibility, queued delete coverage, rename source deletion visibility, queued directory children, queued overlay versions, and queued exact-file versus synthetic-directory conflict policy.
- Queued overlay projection consumes writeback replay snapshots or replay-provided pending facts. Mount runtime and mount projection do not match durable queue record variants directly.
- Queued overlay projection emits mount-visible overlay facts keyed by `MountPath`, not raw strings.
- Durable queue schema remains private to writeback replay. Overlay projection depends on stable pending-write facts, not on serialized queue record shapes.
- The interface is deep only if queue entry shape changes do not force FUSE callbacks, mount runtime, or mount projection to learn new queued operation variants.

Related terms: mount path identity, mount runtime, mount projection, writeback replay, invalidation plan.

## Mount path identity

Definition: The shared path value used inside EventFS once arbitrary user or kernel path input has been validated and normalized into a mount path.

Important invariants:
- `eventfs-protocol::mount_paths::MountPath` owns the normalization invariants: leading slash, no redundant slash, no trailing slash except root, no NUL, no `.` or `..` components.
- FUSE, CLI, or test input may begin as strings or `OsStr` values, but adapter edges convert that input into `MountPath` before it crosses mount runtime, path cache, or invalidation seams.
- Invalidation actions carry `MountPath` values, not raw strings. Matching a cached path against an invalidation action compares already-normalized mount path identities.
- Path cache keys and inode lookups use `MountPath` values internally. String rendering is for FUSE replies, diagnostics, snapshots, JSON output, and storage adapter calls that require strings.
- Mount projection, queued overlay logic, and mount runtime compare `MountPath` values rather than re-normalizing raw strings in multiple modules.

Related terms: materialized path mapping, mount runtime, invalidation plan, path cache.

## Mount runtime

Definition: The module seam that owns mount-visible behavior for an EventFS mount while remaining independent of FUSE kernel request and reply types.

Important invariants:
- Mount runtime owns path lookup, stat/read/list outcomes, handle lifecycle, staged writes, create/mkdir/unlink/rename/truncate behavior, metadata file content, writeback replay integration, cache/watch invalidation consumption, and mount-level error classification.
- The FUSE adapter owns kernel request/reply translation, uid/gid extraction, inode-to-mount-path mapping, open flag adaptation, and conversion from runtime outcomes into fuser types.
- Mount runtime speaks in mount paths, runtime handle identifiers, runtime attributes, runtime directory entries, runtime mutations, and runtime errors. It does not expose `fuser::Request`, `fuser::Reply*`, `fuser::FileAttr`, or `fuser::FileType`.
- Mount projection is internal to mount runtime unless another real caller needs that seam. Tests for normal mount behavior should cross the mount runtime interface rather than private projection helpers.
- The interface is deep only if FUSE callbacks no longer need to know materialized alias rules, queued overlay policy, handle staging, writeback replay state, storage path classification, or cache invalidation details.

Related terms: mount projection, mount path, writeback replay, invalidation plan, FUSE adapter.

## Writeback replay

Definition: The module seam that owns durable pending-write lifecycle from queue persistence through deterministic replay and idempotency proof.

Important invariants:
- Writeback replay owns durable queue schema, replay ordering, applied progress, crash-window recovery, idempotency checks, and whether new writes may be accepted.
- A writeback queue directory has exactly one live owner. Opening the queue
  acquires an OS-level exclusive lock file before reading, compacting, replaying,
  or rewriting `writeback.jsonl`; duplicate mounts or duplicate startups using
  the same cache directory fail clearly until the owner exits.
- Writeback replay owns construction of durable queue entries from planned write intent, payload bytes, and idempotency identity. FUSE adapts mount writes into replay inputs; it does not construct queue records by matching transport operations directly.
- Replay execution owns queued operation dispatch. Callers must not match on queued operation variants to prove, execute, or refresh replay progress.
- Rename completion is modeled as one replay behavior: make the destination durable, then conditionally delete the source using captured source-generation evidence. The two storage actions are internal implementation details of the replay module.
- The durable queue schema may be replaced without migration or compatibility code for pre-release artifacts.
- JetStream and memory storage are production adapters behind the replay seam and must satisfy the same replay, idempotency, error, and evidence contracts. Memory storage may use simpler internals, but it must not rely on fake-only replay shortcuts.
- Restart and reopen behavior must be testable through the replay interface, not only through private helpers.

Related terms: writeback queue, storage adapter, applied operation, idempotency key, mount projection.

## Failed write

Definition: The replay-facing input produced when a mount write cannot be made durable synchronously and must enter writeback replay.

Important invariants:
- A failed write carries the planned JetStream action, payload bytes, idempotency identity, source-generation evidence when required, and a mount-visible path for diagnostics.
- A failed write is not the durable queue schema. Writeback replay translates failed writes into private durable records.
- FUSE may create failed-write inputs from mount callbacks, but FUSE must not construct durable queue records directly.
- Failed writes preserve the caller's intent without exposing how replay will prove or execute that intent.

Related terms: writeback replay, writeback queue, mount projection, idempotency key.

## Replay store (`ReplayStorage`)

Definition: The storage adapter interface consumed by writeback replay to prove and execute queued writes.

Important invariants:
- `ReplayStorage` is the concrete replay store trait.
- Replay store is the only storage seam crossed by writeback replay.
- Replay store supports both JetStream and memory adapters with the same behavioral contract.
- Replay store exposes replay evidence and execution behavior at the level writeback replay needs, not FUSE-facing storage operations.
- Replay store is internal to the writeback replay seam unless more than one caller genuinely needs it.

Related terms: storage adapter, writeback replay, applied operation, idempotency key.

## NATS core module

Definition: The private storage adapter module that owns shared NATS connection mechanics for the `NatsStorage` adapter implementation.

Important invariants:
- The NATS core module owns the JetStream context, adapter configuration, retry policy, timeout policy, stream configuration helpers, duplicate-window setup, and NATS error mapping.
- Primitive modules use NATS core for broker mechanics, but they own primitive behavior. NATS core does not know object marker schemas, KV marker schemas, stream line proof, watch dispatch, or mount-visible storage semantics.
- `NatsStorage` remains the public adapter and wiring point. NATS core is an implementation detail, not a new public storage seam.
- The module is deep only if retry, timeout, stream configuration, and error mapping changes can be made once without pushing raw NATS mechanics into KV, stream, object, ledger, or watch modules.

Related terms: storage adapter, NATS KV module, NATS stream module, NATS object module, NATS watch module, writeback ledger.

## NATS KV module

Definition: The private storage adapter module that owns NATS Key-Value behavior inside the `NatsStorage` adapter implementation.

Important invariants:
- The NATS KV module owns KV bucket discovery, bucket creation, key reads, writes, guarded deletes, key history, history-prefix listing, KV watch probes, and KV writeback marker encoding.
- KV replay proof stays primitive-owned. The module owns KV marker subjects, marker payload validation, idempotency header checks, pre-marker supersession checks, and KV-specific applied ledger keys.
- Callers cross `MountStorage` and `ReplayStorage`; they do not learn KV marker keys, delete expected-sequence publishing, or KV history scan details.
- The module is deep only if KV replay-marker bugs and KV delete semantics can be changed without loading stream JSONL, object NUID cleanup, or watch dispatch code.

Related terms: storage adapter, replay store, writeback replay, NATS core module, writeback ledger.

## NATS stream module

Definition: The private storage adapter module that owns NATS stream and JSONL subject behavior inside the `NatsStorage` adapter implementation.

Important invariants:
- The NATS stream module owns stream discovery, stream creation, subject filter updates, retained subject discovery, message listing, message reads, JSONL validation/publishing, stream-line idempotency proof, and agent mailbox subject classification.
- Stream replay proof stays primitive-owned. The module owns message-id construction and stream-specific applied ledger keys while the generic ledger owns physical applied fact storage.
- Callers cross `MountStorage` and `ReplayStorage`; they do not learn pull-consumer setup, header-only scan behavior, subject-filter mutation, or mailbox subject parsing.
- The module is deep only if stream subject discovery, JSONL replay progress, and mailbox listing behavior can change without loading KV marker or object chunk-cleanup code.

Related terms: storage adapter, replay store, writeback replay, NATS core module, writeback ledger.

## NATS object module

Definition: The private storage adapter module that owns NATS Object Store behavior inside the `NatsStorage` adapter implementation.

Important invariants:
- The NATS object module owns object bucket layout, metadata subjects, chunk subjects, object reads, object writes, guarded object deletes, object writeback marker encoding, object idempotency descriptions, previous-NUID recovery, and chunk purge rules.
- Object writeback proof stays primitive-owned. The module may construct object-specific applied ledger keys, but the physical ledger read/write behavior remains in the generic writeback ledger module.
- `NatsStorage` remains the public adapter. Callers continue to cross `MountStorage` and `ReplayStorage`; they do not learn object marker subjects, NUID cleanup, or crash-window proof ordering.
- The module is deep only if object replay cleanup bugs and object idempotency changes can be fixed without loading KV marker, stream message-id, or watch dispatch implementation details.

Related terms: storage adapter, replay store, writeback replay, applied operation, idempotency key.

## NATS watch module

Definition: The private storage adapter module that owns live NATS subscription setup and watch dispatch inside the `NatsStorage` adapter implementation.

Important invariants:
- The NATS watch module owns connection continuity callbacks, live subscriptions, dropped-message gap detection, bounded drain behavior, and conversion from raw NATS subjects into mount-visible watch events.
- Watch dispatch consumes primitive-owned subject parsers for KV, streams, and objects, then maps storage facts through the shared materialized path mapping.
- Chunk traffic and hidden writeback marker traffic are ignored without becoming mount-visible invalidations, but drain cycles remain bounded and report a gap when the bound is reached.
- The module is deep only if watch continuity and subject dispatch changes can be made without editing KV replay, stream publishing, or object chunk-cleanup behavior.

Related terms: storage adapter, NATS KV module, NATS stream module, NATS object module, materialized path mapping, invalidation plan.

## Writeback ledger

Definition: The private NATS adapter module that records and reads generic applied-operation facts for replay crash-window recovery.

Important invariants:
- The writeback ledger stores opaque applied-operation keys and verifies matching payloads. It does not own KV key semantics, stream subject semantics, object names, object NUIDs, or marker payload schemas.
- Primitive modules own construction of their ledger keys because the identity inputs are primitive-specific.
- The ledger module owns the physical NATS stream, subject encoding, duplicate-window setup, publish/read behavior, and not-found handling for applied facts.

Related terms: writeback replay, replay store, storage adapter, applied operation, idempotency key.

## Invalidation plan

Definition: The module seam that converts local mutations, watch events, replay outcomes, and gap events into cache and path-cache actions.

Important invariants:
- The same invalidation plan feeds transport cache state and FUSE path-cache state.
- Storage fact expansion remains based on `eventfs-protocol::mount_paths`; the plan adds action semantics such as invalidate exact path, invalidate ancestors, clear all on gap, or refresh known metadata.
- Watch continuity rules and gap policy are owned in one place so stale inode behavior is localized.

Related terms: materialized path mapping, affected path, watch invalidation, cache, path cache.

## Storage adapter

Definition: A production-capable implementation of EventFS storage operations behind stable storage seams.

Important invariants:
- NATS JetStream is the primary production adapter.
- Memory storage is also production-capable for embedded, local, and test deployments when it can satisfy the same contracts without fake-only shortcuts.
- Additional former fake adapters may become production adapters only when they satisfy the same behavior, error, replay, and observability contracts as the concrete seam requires.
- The transport-facing storage adapter facade exposes stable storage behavior while primitive-specific implementation lives behind private deep modules for KV, streams, objects, watches, cache invalidation, and writeback/replay support.
- Split storage seams by real caller variation and test leverage; avoid pass-through traits that merely rename JetStream methods.
- `eventfs-transport/src/storage/mod.rs` should stay navigable as the storage facade and wiring point. It should not require maintainers to load unrelated NATS primitive knowledge to change one storage behavior.
- `eventfs-transport/src/storage/nats/mod.rs` should stay a concrete `NatsStorage` wiring module over private NATS core, KV, stream, object, watch, and ledger modules.
- Shared adapter contract tests prove behavior at the storage seam, including replay behavior where writeback depends on storage evidence.

Related terms: NATS storage adapter, memory storage adapter, replay store, writeback replay, mount projection.

## Direct JetStream data plane

Definition: The runtime topology where the FUSE mount talks to NATS JetStream through the transport adapter without an intermediate storage process.

Important invariants:
- `eventfs-protocol`, `eventfs-transport`, and `eventfs-fuse` are the supported workspace crates for the active data plane.
- Supported command paths start the FUSE mount and a JetStream-enabled NATS broker.
- Retired session-control artifacts and standalone storage-process commands are not part of the supported runtime workflow.

Related terms: storage adapter, mount projection, writeback replay, invalidation plan.
