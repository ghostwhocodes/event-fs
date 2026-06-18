# Development

EventFS is developed as a direct FUSE-to-JetStream mount. Keep path and
operation semantics in `eventfs-protocol`, JetStream IO in `eventfs-transport`,
and kernel adaptation in `eventfs-fuse`.

## Local Setup

Install the usual Rust toolchain plus system support for FUSE. On Linux, the
smoke flow needs `/dev/fuse` and an unmount helper such as `fusermount3`.

Start a local NATS broker with JetStream enabled:

```sh
docker compose -f infra/nats/docker-compose.yml up -d
```

Build and test:

```sh
cargo fmt --all
cargo test --workspace
just check
```

Run broker-backed tests:

```sh
NATS_URL=nats://127.0.0.1:4222 cargo test --workspace --features jetstream-tests
```

Run the mount:

```sh
mkdir -p /tmp/eventfs
cargo run -p eventfs-fuse -- /tmp/eventfs nats://127.0.0.1:4222
```

Run the smoke flow:

```sh
NATS_URL=nats://127.0.0.1:4222 ./smoke-eventfs.sh
```

## Design Rules

- Add shared path, subject, JSON, alias, and errno behavior to
  `eventfs-protocol`.
- Keep NATS calls, retries, watches, cache entries, and writeback replay in
  `eventfs-transport`.
- Keep FUSE callbacks thin. The FUSE layer should translate kernel requests
  into mount runtime operations and map errors back to errno values.
- Keep durable queue records private to `eventfs-transport::writeback`.
  Callers should pass `FailedWrite` values and read diagnostic snapshots.
- Use `eventfs_protocol::mount_paths` when a change affects native paths and
  materialized aliases. Do not duplicate alias expansion in FUSE or transport
  code.
- Add pure tests for path planning, JSON validation, invalidation, cache, and
  writeback behavior before adding broker-backed tests.

## Common Workflows

Format:

```sh
just fmt
```

Run one focused Rust test:

```sh
just test-one eventfs_path
```

Run all local checks:

```sh
just check
```

`just check` is the product gate: format check, clippy, and Rust workspace
tests. Source-repository maintainers can run Codex workflow runtime tests
separately with `just codex-test`.

Run integration tests against a real broker:

```sh
just integration
```

Run the FUSE smoke test:

```sh
just smoke
```

Unmount a local mount:

```sh
fusermount3 -u /tmp/eventfs
```

## Documentation

Update docs when behavior or interfaces change:

- `README.md`: project overview and quick start.
- `docs/USAGE.md`: newcomer usage guide.
- `docs/ARCHITECTURE.md`: crate boundaries, runtime flow, queue, cache, and
  diagnostics.
- `docs/PROTOCOL.md`: mount path grammar, operation planning, aliases, and
  errno rules.
- `docs/COMMANDS.md`: supported commands and scripts.
- `docs/TESTING.md`: validation layers and command matrix.
- `docs/SESSION_LAYOUT_AND_CLI.md`: current mounted layout and CLI contract.
