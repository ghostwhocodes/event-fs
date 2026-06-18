# Protocol

`eventfs-protocol` defines the local contract between FUSE, transport, tests,
and docs. It performs no IO.

## Path Model

Every mount path parses into a typed `JetStreamPath`. The supported root
directories are:

```text
/kv
/streams
/objects
/events
/tasks
/agents
/semantic
/.eventfs
```

Bucket, stream, task namespace, agent, and mount-root names use ASCII
alphanumeric characters plus `_` and `-`. Generic mount components reject NUL,
empty components, `.`, `..`, and leading `.`. KV keys may contain ASCII
letters, digits, `-`, `_`, `=`, `.`, and `/`, but may not be empty, start with
`.`, end with `.`, or use reserved internal prefixes.

## Native Paths

| Path | Parsed variant | Notes |
| --- | --- | --- |
| `/kv/<bucket>/<key-path>` | `KvKey` | Current KV value |
| `/kv/<bucket>/.history/<key-path>/@<revision>` | `KvRevision` | Immutable KV revision |
| `/streams/<stream>/messages/<sequence>.json` | `StreamMessage` | Sequence must be numeric |
| `/streams/<stream>/subjects/<subject-file>.jsonl` | `StreamSubject` | Subject filename decodes to a NATS subject |
| `/objects/<bucket>/<object-path>` | `Object` | Whole-object path |

KV history uses `@<revision>` so numeric key path segments remain part of the
key unless the final component has the revision marker.

Stream subject file names encode NATS subjects. Path-safe subjects such as
`orders.created` appear as `orders.created.jsonl`. Subjects that are not safe
as one path component use a hex filename with the `__eventfs_subject_hex_` prefix.

## Materialized Paths

Materialized paths map to deterministic JetStream targets:

| Mount path | Backing target |
| --- | --- |
| `/events/<stream>.jsonl` | Stream `<stream>`, subject `events.<stream>` |
| `/tasks/<namespace>/<task>.json` | KV bucket `EVENTFS_TASKS`, key `<namespace>/<task>.json` |
| `/agents/<agent>/inbox` | Stream `EVENTFS_AGENTS`, subject `agents.<agent>.inbox` |
| `/agents/<agent>/outbox` | Stream `EVENTFS_AGENTS`, subject `agents.<agent>.outbox` |
| `/agents/<agent>/tasks/<path>` | KV bucket `EVENTFS_AGENTS`, key `<agent>/tasks/<path>` |
| `/agents/<agent>/memory/<path>` | KV bucket `EVENTFS_AGENTS`, key `<agent>/memory/<path>` |
| `/semantic/<area>/<path>` | KV bucket `EVENTFS_SEMANTIC`, key `<area>/<path>` |

Semantic areas are `summaries`, `tags`, `relations`, `timelines`, and
`embeddings`.

## Operation Planning

The planner accepts a filesystem intent and a parsed path, then returns either
a typed `JetStreamAction` or a protocol error with an errno.

| Intent | Examples |
| --- | --- |
| `GetAttr` | Directory paths become `NoopDirectory`; file paths plan the matching read action |
| `ReadDir` | Root returns static entries; dynamic roots plan KV, stream, object, event, agent, or semantic listings |
| `Read` | KV get, KV revision read, stream message read, object get, materialized get, or metadata read |
| `Write` / `Create` | KV put, object put, materialized KV put, or JSONL publish |
| `Mkdir` | Ensure KV bucket, stream, object bucket, or synthetic directory prefix |
| `Unlink` | KV delete, object delete, or materialized KV delete |
| `Rename` | Same-bucket KV rename, same-bucket object rename, or materialized KV rename |

Cross-surface rename returns `EXDEV`. Immutable projections return `EROFS`.
Invalid paths and malformed JSON return `EINVAL`. Unsupported operations return
`ENOTSUP` unless a more specific errno applies.

## JSON Rules

`validate_json_document` validates materialized single-document surfaces such
as tasks, agent records, and semantic records. Native KV and object writes are
byte-oriented and are not forced through JSON validation.

`validate_json_lines` validates append surfaces. Each non-empty line must be a
complete JSON value. JSONL writes are queued or published only after validation.

## Mount Path Mapping

`eventfs-protocol::mount_paths` maps storage facts into mount-visible paths:

- `StorageFact::Kv { bucket, key }`
- `StorageFact::StreamSubject { stream, subject }`
- `StorageFact::Object { bucket, name }`

`visible_paths` returns file paths visible through the mount. Native storage
paths may also produce aliases. For example:

- `EVENTFS_TASKS/demo/render.json` is visible at
  `/kv/EVENTFS_TASKS/demo/render.json` and `/tasks/demo/render.json`.
- Stream `system` subject `events.system` is visible at
  `/streams/system/subjects/events.system.jsonl` and `/events/system.jsonl`.
- Stream `EVENTFS_AGENTS` subject `agents.bot.inbox` is visible at the
  native stream subject path and `/agents/bot/inbox`.

`invalidation_paths` returns visible paths plus ancestor prefixes that must be
reclassified after a mutation or watch event. Each result includes an
`AffectedPathReason`:

- `Exact`: native storage path.
- `Alias`: materialized alias.
- `Mailbox`: agent mailbox alias.
- `Ancestor`: directory prefix that may change kind or listing contents.

`eventfs-transport::InvalidationPlan` converts these affected paths into
exact-entry, subtree, or full-clear actions for caches.
