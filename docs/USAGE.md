# Usage

This guide starts from a fresh checkout and a local NATS broker. It assumes a
Linux host with FUSE support.

## 1. Start NATS

```sh
docker compose -f infra/nats/docker-compose.yml up -d
```

The Compose service starts NATS with JetStream enabled.

## 2. Mount EventFS

```sh
mkdir -p /tmp/eventfs
cargo run -p eventfs-fuse -- /tmp/eventfs nats://127.0.0.1:4222
```

The process stays in the foreground. Open another shell for filesystem
operations.

Use credentials when your broker requires them:

```sh
NATS_CREDS_FILE=/path/to/user.creds \
  cargo run -p eventfs-fuse -- /tmp/eventfs nats://broker.example:4222
```

Use an explicit queue/cache directory when you want repeatable replay state:

```sh
cargo run -p eventfs-fuse -- /tmp/eventfs nats://127.0.0.1:4222 \
  --mount-name dev \
  --cache-dir /tmp/eventfs-cache
```

## 3. Write KV Data

KV buckets appear under `/kv`.

```sh
mkdir -p /tmp/eventfs/kv/config
printf '{"service":"api","enabled":true}' >/tmp/eventfs/kv/config/service.json
cat /tmp/eventfs/kv/config/service.json
```

Read a stored revision through history:

```sh
ls /tmp/eventfs/kv/config/.history/service.json
cat /tmp/eventfs/kv/config/.history/service.json/@1
```

## 4. Publish Events And Stream Messages

Append JSON lines to an event log:

```sh
printf '{"kind":"deploy","status":"started"}\n' >>/tmp/eventfs/events/system.jsonl
```

The event path publishes to stream `system` on subject `events.system`.

Append directly to a stream subject:

```sh
mkdir -p /tmp/eventfs/streams/system/subjects
printf '{"kind":"deploy","status":"done"}\n' \
  >>/tmp/eventfs/streams/system/subjects/events.system.jsonl
```

Read immutable stream messages:

```sh
ls /tmp/eventfs/streams/system/messages
cat /tmp/eventfs/streams/system/messages/1.json
```

## 5. Store Objects

Object buckets appear under `/objects`.

```sh
mkdir -p /tmp/eventfs/objects/assets
printf 'binary or text payload' >/tmp/eventfs/objects/assets/readme.txt
cat /tmp/eventfs/objects/assets/readme.txt
```

Object writes replace the complete object on `fsync` or file close.

## 6. Use JSON-First Agent Paths

Tasks are JSON files backed by the `EVENTFS_TASKS` KV bucket:

```sh
mkdir -p /tmp/eventfs/tasks/demo
printf '{"task":"render","state":"new"}' >/tmp/eventfs/tasks/demo/render-001.json
```

Agent mailboxes are JSONL files backed by the `EVENTFS_AGENTS` stream:

```sh
mkdir -p /tmp/eventfs/agents/bot
printf '{"from":"user","body":"hello"}\n' >>/tmp/eventfs/agents/bot/inbox
```

Agent task and memory records are KV-backed JSON paths:

```sh
mkdir -p /tmp/eventfs/agents/bot/memory/facts
printf '{"fact":"runs on JetStream"}' >/tmp/eventfs/agents/bot/memory/facts/eventfs.json
```

Semantic metadata is KV-backed under fixed areas:

```sh
mkdir -p /tmp/eventfs/semantic/tags/project
printf '{"tags":["eventfs","jetstream"]}' >/tmp/eventfs/semantic/tags/project/eventfs.json
```

## 7. Inspect Diagnostics

```sh
cat /tmp/eventfs/.eventfs/status.json
cat /tmp/eventfs/.eventfs/cache.json
cat /tmp/eventfs/.eventfs/queue.json
cat /tmp/eventfs/.eventfs/capabilities.json
cat /tmp/eventfs/.eventfs/errors.jsonl
```

`status.json` shows whether writes are accepted. If startup replay leaves
pending queue entries, the mount enters a read-only state until replay drains.

## 8. Run The Smoke Test

```sh
NATS_URL=nats://127.0.0.1:4222 ./smoke-eventfs.sh
```

The smoke script mounts a temporary filesystem, writes through the main
surfaces, verifies broker state, and unmounts.

## 9. Unmount

```sh
fusermount3 -u /tmp/eventfs
```

Use `fusermount -u /tmp/eventfs` or `umount /tmp/eventfs` when appropriate for
your system.

## Behavior To Expect

- KV and object files are whole-value files. Partial writes are staged and
  committed on `fsync` or close.
- JSONL surfaces publish complete JSON lines. A trailing newline keeps shell
  appends readable, but validation is based on complete JSON values.
- Stream messages and KV history entries are read-only.
- Hard links, distributed locks, mutable stream messages, and cross-surface
  rename are unsupported.
- Pending writeback entries are visible in `/.eventfs/queue.json`.
