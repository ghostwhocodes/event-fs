# Architecture

EventFS is a direct FUSE projection over NATS JetStream. The broker owns durable
state. The mount process translates filesystem operations into typed protocol
actions and JetStream adapter calls.

## Process Boundaries

```text
Linux kernel FUSE API
        |
eventfs-fuse
        |
eventfs-protocol
        |
eventfs-transport
        |
NATS JetStream
```

`eventfs-fuse` runs as the only EventFS process in the data path. It connects
to NATS, opens the durable writeback queue, and blocks in `fuser::mount2` until
the mount is unmounted.

## Crate Ownership

| Crate | Owns | Does not own |
| --- | --- | --- |
| `eventfs-protocol` | Path parsing, operation planning, materialized aliases, JSON validation, errno choices | NATS IO, kernel replies, queue files |
| `eventfs-transport` | Mount/replay storage interfaces, NATS storage adapter, memory storage adapter, cache entries, watch events, invalidation plans, durable writeback replay | FUSE inode state, mount path lookup, kernel errno replies |
| `eventfs-fuse` | FUSE callbacks, inode/path cache, mount runtime, staged handles, diagnostics, kernel error mapping | Path contract decisions, raw NATS subject parsing, durable queue schema construction |

The protocol crate is the shared contract. Transport and FUSE adapt their local
inputs into protocol types instead of duplicating path grammar or alias rules.
Inside the NATS storage adapter, `NatsStorage` is the concrete wiring point over
private core, KV, stream, object, watch, and writeback ledger modules.

## Runtime Flow

```text
FUSE request
  -> PathCache resolves inode to MountPath
  -> JetStreamPath::parse validates the mount path
  -> plan_operation maps intent to JetStreamAction
  -> MountRuntime handles cache, staging, write gates, and errors
  -> MountStorage executes the adapter operation
  -> InvalidationPlan marks affected cache and inode paths stale
```

`eventfs-fuse::fs::runtime` is the mount behavior layer. It owns stat, read,
directory listing, handle lifecycle, staged writes, create, mkdir, unlink,
rename, diagnostics, startup queue replay, watch consumption, and mount-level
error recording. It speaks in mount paths, runtime handles, runtime attributes,
runtime directory entries, and runtime mutations. It does not expose FUSE reply
types.

`eventfs-fuse::fs::mount_projection` classifies mount paths as files or
directories, reads durable snapshots, merges queued write overlays into visible
results, and chooses write modes. Append surfaces use JSONL write mode; KV,
object, task, agent-record, and semantic-record surfaces use whole-value mode.

## Root Layout

| Path | Backing primitive | Semantics |
| --- | --- | --- |
| `/kv/<bucket>/<key-path>` | JetStream KV | Current KV value as a file |
| `/kv/<bucket>/.history/<key-path>/@<revision>` | JetStream KV history | Immutable revision read |
| `/streams/<stream>/messages/<sequence>.json` | JetStream stream message | Immutable message projection |
| `/streams/<stream>/subjects/<subject-file>.jsonl` | JetStream stream publish/read projection | Append complete JSON lines to a subject |
| `/objects/<bucket>/<object-path>` | JetStream Object Store | Whole-object read/write |
| `/events/<stream>.jsonl` | JetStream stream subject `events.<stream>` | Event log alias |
| `/tasks/<namespace>/<task>.json` | KV bucket `EVENTFS_TASKS` | JSON task record |
| `/agents/<agent>/inbox` | Stream `EVENTFS_AGENTS`, subject `agents.<agent>.inbox` | JSONL mailbox |
| `/agents/<agent>/outbox` | Stream `EVENTFS_AGENTS`, subject `agents.<agent>.outbox` | JSONL mailbox |
| `/agents/<agent>/{tasks,memory}/<path>` | KV bucket `EVENTFS_AGENTS` | Agent JSON records |
| `/semantic/<area>/<path>` | KV bucket `EVENTFS_SEMANTIC` | Tool-produced metadata |
| `/.eventfs/*` | Local mount state | Diagnostics |

KV keys and object names are projected as slash-separated synthetic
directories. If JetStream contains both an exact value and descendant entries at
the same mount path, the exact value is exposed as the file. The descendants
remain in JetStream but cannot also be represented as a POSIX directory at that
path.

## Commit Model

- Opening a writable whole-value file stages bytes in the handle buffer.
- `fsync` and `release` commit staged whole-value buffers to KV or Object Store.
- `flush` validates the handle and returns without committing whole-value data.
- Stream subject files, `/events/*.jsonl`, and agent mailboxes publish one
  message per complete JSON line.
- A handle commits once. Later `flush`, `fsync`, or `release` calls for that
  handle do not publish duplicates.
- JSON surfaces validate records before publish or queueing.
- Stream messages and KV history revisions are read-only.
- Append-oriented JSONL surfaces reject truncate.

## Durable Writeback

`eventfs-transport::writeback` owns the queue file format and replay lifecycle.
The queue is a bounded `writeback.jsonl` file in the mount cache directory. The
queue directory also has a `writeback.lock` file; a second process using the
same cache directory fails startup instead of opening the queue concurrently.

The queue accepts failed KV puts, KV rename completions, object puts, object
rename completions, JSONL publishes, and materialized KV puts. It exposes a
diagnostic snapshot with operation kind, target, payload length, attempts, and
idempotency key, but callers do not construct or match private durable queue
records.

Replay rules:

- Startup replay runs before the mount accepts new writes.
- If replay drains the queue, normal writes are enabled.
- If entries remain pending, the mount reports a read-only state and new writes
  fail with `EROFS` until replay succeeds.
- Stream JSONL replay tracks applied-line progress.
- KV and object replay use idempotency keys plus backend markers or metadata to
  detect work already applied across crash windows.
- Queue rewrites use a synced temporary file, atomic rename, and directory
  fsync.
- A torn final JSONL record is ignored; corrupt complete records fail queue
  open.

## Cache And Invalidation

The transport cache stores file bytes and directory projections with an origin
revision or sequence. The FUSE path cache maps inode numbers to normalized
mount paths. Both caches consume `InvalidationPlan` actions:

- `ExactEntry` removes one cached path decision.
- `Subtree` removes a path and descendants.
- `All` clears all non-root paths after a watch gap.

Watch events and local mutations are converted into affected paths through
`eventfs-protocol::mount_paths`. That module emits native paths, materialized
aliases, mailbox aliases, and ancestor prefixes from one storage fact. The
transport layer remains responsible for NATS-specific subject parsing.

## Diagnostics

`/.eventfs` exposes local JSON diagnostics:

- `status.json`: mount name, state, write gate, and pending queue count.
- `cache.json`: FUSE path-cache size, transport-cache size, and cache
  generation.
- `queue.json`: queue capacity, pending count, done count, and pending entries.
- `capabilities.json`: supported roots and feature flags.
- `errors.jsonl`: recent mount errors, one JSON object per line.

## Unsupported POSIX Behavior

EventFS does not emulate mutable block storage. Hard links, distributed locks,
`mmap` coherence, byte-range atomicity, mutable stream messages, and
cross-surface rename return explicit errno values.
