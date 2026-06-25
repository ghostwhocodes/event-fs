# EventFS

<p align="center">
  <img src="./.github/assets/banner.webp" alt="EventFS banner">
</p>

<p align="center">
  <a href="https://ghost-who-codes.blog/open-source/event-fs/">Project page on Ghost Who Codes</a>
</p>

EventFS mounts NATS JetStream as a filesystem. JetStream is the system of
record; the FUSE process is the local interface for shells, containers, humans,
and agents.

The current workspace implements a direct FUSE-to-JetStream mount. There is no
separate filesystem server in the data path. The executable is `eventfs-fuse`.

## Workspace

- `eventfs-protocol`: pure mount path parsing, operation planning, materialized
  path mapping, JSON validation helpers, and errno decisions.
- `eventfs-transport`: NATS JetStream adapter, in-memory adapter for tests,
  local cache, invalidation planning, watch adaptation, and durable writeback
  queue.
- `eventfs-fuse`: FUSE callback adapter, inode/path cache, mount runtime,
  staged writes, metadata files, and kernel errno mapping.
- `infra/nats`: local NATS server with JetStream enabled.

## Mounted Layout

```text
/
  kv/<bucket>/<key-path>
  kv/<bucket>/.history/<key-path>/@<revision>
  streams/<stream>/messages/<sequence>.json
  streams/<stream>/subjects/<subject-file>.jsonl
  objects/<bucket>/<object-path>
  events/<stream>.jsonl
  tasks/<namespace>/<task>.json
  agents/<agent>/{inbox,outbox,tasks,memory}
  semantic/{summaries,tags,relations,timelines,embeddings}
  .eventfs/{status.json,cache.json,queue.json,capabilities.json,errors.jsonl}
```

The native roots expose JetStream KV, stream, and object store data. The
`events`, `tasks`, `agents`, and `semantic` roots are materialized JSON-first
views backed by deterministic JetStream streams or KV buckets.

## Quick Start

Start a local broker:

```sh
docker compose -f infra/nats/docker-compose.yml up -d
```

Run tests:

```sh
cargo fmt --all
cargo test --workspace
just check
```

Mount locally:

```sh
mkdir -p /tmp/eventfs
cargo run -p eventfs-fuse -- /tmp/eventfs nats://127.0.0.1:4222
```

Write data through the mount:

```sh
mkdir -p /tmp/eventfs/kv/demo
printf '{"hello":"kv"}' >/tmp/eventfs/kv/demo/greeting.json

printf '{"kind":"event","n":1}\n' >>/tmp/eventfs/events/system.jsonl

mkdir -p /tmp/eventfs/objects/demo
printf 'object payload' >/tmp/eventfs/objects/demo/payload.txt

cat /tmp/eventfs/.eventfs/status.json
```

Unmount when finished:

```sh
fusermount3 -u /tmp/eventfs
```

Use `fusermount -u` or `umount` if your platform does not provide
`fusermount3`.

## Write Semantics

- KV files and object paths are whole-value records. Writes are staged per file
  handle and committed on `fsync` or `release`.
- `flush` validates the open handle but does not make whole-value writes
  durable.
- Stream subject files, event logs, and agent mailboxes are append-oriented
  JSONL surfaces. Each complete JSON line becomes one JetStream message.
- Stream message files and KV history revision files are immutable projections.
- Append-oriented JSONL files reject truncation because JetStream messages are
  immutable.
- Eligible writes that fail while JetStream is unavailable enter a bounded
  durable queue. Replay uses idempotency keys and backend evidence to avoid
  duplicate KV/object writes or duplicate stream lines.
- Unsupported POSIX behavior, including hard links, byte-range mutation
  guarantees, distributed locks, mutable stream messages, and cross-surface
  rename, fails explicitly.

## Local State

By default, the durable writeback queue lives under:

- `$XDG_STATE_HOME/eventfs/mounts`
- `$HOME/.local/state/eventfs/mounts`
- `.eventfs-state/mounts` under the current directory when no state home
  exists

The derived queue directory includes the mount name, mountpoint, NATS URL, and
credentials path. Use `--cache-dir <DIR>` to pin a queue directory for a mount.

## Documentation

- [Usage](docs/USAGE.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Protocol](docs/PROTOCOL.md)
- [Commands](docs/COMMANDS.md)
- [Development](docs/DEVELOPMENT.md)
- [Testing](docs/TESTING.md)
- [Mount layout and CLI contract](docs/SESSION_LAYOUT_AND_CLI.md)

## License

EventFS is licensed under the GNU Affero General Public License v3.0 only
(`AGPL-3.0-only`). See [LICENSE](LICENSE) for the full text and
[COPYRIGHT](COPYRIGHT) for the repository notice. This program is distributed
without any warranty.
