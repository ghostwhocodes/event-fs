# Mount Layout And CLI Contract

This file describes the supported filesystem layout and command-line surface in
the current workspace. The workspace contains `eventfs-protocol`,
`eventfs-transport`, and `eventfs-fuse`; it does not define a separate session
server or control-plane CLI.

## Mounted Layout

```text
/
  kv/
    <bucket>/
      <key-path>
      .history/<key-path>/@<revision>
  streams/
    <stream>/
      messages/<sequence>.json
      subjects/<subject-file>.jsonl
  objects/
    <bucket>/<object-path>
  events/
    <stream>.jsonl
  tasks/
    <namespace>/<task>.json
  agents/
    <agent>/
      inbox
      outbox
      tasks/<path>
      memory/<path>
  semantic/
    summaries/<path>
    tags/<path>
    relations/<path>
    timelines/<path>
    embeddings/<path>
  .eventfs/
    status.json
    cache.json
    queue.json
    capabilities.json
    errors.jsonl
```

## CLI

```text
eventfs-fuse <MOUNTPOINT> [NATS_URL]
  --nats-creds-file <PATH>
  --mount-name <NAME>
  --cache-dir <DIR>
  --timeout-ms <N>
  --duplicate-window-ms <N>
  --queue-capacity <N>
  --foreground
```

Defaults:

| Option | Default |
| --- | --- |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `--mount-name` | `eventfs` |
| `--timeout-ms` | `2000` |
| `--duplicate-window-ms` | `86400000` |
| `--queue-capacity` | `1024` |

`--nats-creds-file` can also be supplied through `NATS_CREDS_FILE`.
`--foreground` is accepted by the parser; the current mount process already
runs in the foreground because `fuser::mount2` blocks.

## Local State Contract

If `--cache-dir` is not supplied, the mount derives a per-mount state directory
under the first available root:

1. `$XDG_STATE_HOME/eventfs/mounts`
2. `$HOME/.local/state/eventfs/mounts`
3. `.eventfs-state/mounts` under the current directory

The identity hash includes:

- mount name
- NATS URL
- mountpoint
- credentials path

The queue directory contains a private `writeback.jsonl` file and an exclusive
`writeback.lock`. One live mount process may own a queue directory.

## Diagnostics Contract

Diagnostics are local to the mount and do not require extra JetStream data.

`/.eventfs/status.json` includes:

- `mount_name`
- `state`, either `mounted` or `read_only`
- `writes_blocked`
- `queue_pending`

`/.eventfs/cache.json` includes:

- FUSE path-cache entry count
- transport cache entry count
- cache generation

`/.eventfs/queue.json` includes:

- queue capacity
- pending count
- done count
- pending entry summaries

`/.eventfs/capabilities.json` lists supported roots and feature flags.
`/.eventfs/errors.jsonl` contains recent mount errors as JSONL.

## Helper Scripts

`./run-fuse.sh <mountpoint> [nats-url] [eventfs-fuse args...]` starts the mount
through Cargo. It passes `NATS_CREDS_FILE` through when set.

`./smoke-eventfs.sh` mounts a temporary filesystem, writes representative
KV, stream, object, task, and diagnostic paths, verifies broker state with
`eventfs-probe`, and unmounts.

`cargo run -p eventfs-transport --bin eventfs-probe -- [NATS_URL]` verifies
the broker state created by the smoke script.

## Reserved Names

The current mount surface and internal broker resources use the EventFS prefix:

- `/.eventfs`
- default mount name `eventfs`
- KV buckets `EVENTFS_TASKS`, `EVENTFS_AGENTS`, and
  `EVENTFS_SEMANTIC`
- stream `EVENTFS_AGENTS`
- writeback ledger stream `EVENTFS_WRITEBACK`
