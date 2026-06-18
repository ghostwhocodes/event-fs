# Testing

EventFS validation has four layers.

## Pure Workspace Tests

Run the full Rust test suite without a broker:

```sh
cargo test --workspace
```

These tests cover path parsing, operation planning, subject filename encoding,
JSON validation, materialized path mapping, invalidation plans, local cache
behavior, writeback queue persistence, FUSE runtime behavior with the memory
adapter, and error mapping.

Run a focused filter:

```sh
cargo test --workspace eventfs_path -- --nocapture
```

## Broker-Backed Tests

Start NATS with JetStream:

```sh
docker compose -f infra/nats/docker-compose.yml up -d
```

Run tests that require a broker:

```sh
NATS_URL=nats://127.0.0.1:4222 cargo test --workspace --features jetstream-tests
```

These tests exercise the real NATS adapter: KV values and history, stream
publish/read behavior, object store blobs, watch invalidation, writeback replay,
idempotency markers, duplicate windows, and broker error mapping.

The Just recipe starts the Compose broker before running the same feature
tests:

```sh
just integration
```

`just integration` resets the `infra/nats` Compose service before starting it
so broker-backed release validation does not inherit stale JetStream state.

## FUSE Smoke

Run the smoke flow when `/dev/fuse` and user mounts are available:

```sh
NATS_URL=nats://127.0.0.1:4222 ./smoke-eventfs.sh
```

The smoke script mounts EventFS, writes through KV, event, stream-subject,
object, task, and diagnostic paths, verifies broker state independently with
`eventfs-probe`, unmounts, and checks that mount state is cleaned up.

Useful environment variables:

- `NATS_URL`: broker URL.
- `MOUNT_NAME`: diagnostic mount name.
- `MOUNTPOINT`: existing mount directory to use.
- `CACHE_DIR`: queue/cache directory to use.

## Repository Checks

Run the local CI set:

```sh
just check
```

`just check` runs:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`

`just check` is the product release gate and does not require repo-local Codex
workflow files. Source-repository maintainers can validate the Codex
long-horizon workflow runtime separately:

```sh
just codex-test
```

Run the release check wrapper:

```sh
just release-check
```

`just release-check` is a local convenience wrapper. It always runs the product
gate, then runs broker-backed integration when Docker Compose and daemon access
are available. FUSE smoke runs only when `/dev/fuse` is available and either
the broker-backed integration step started the local broker or `NATS_URL` was
explicitly supplied for an external broker. Any skipped local check is printed
in the final summary.

Use strict release mode when the result will gate a release:

```sh
just release-check-strict
```

Strict mode fails if Docker with Compose, Docker daemon access, or `/dev/fuse`
is unavailable.

Run the release-surface guard when changing release scripts, CI, or product
docs:

```sh
just release-surface-guard
```

The guard fails if product release recipes or product-facing docs reference
Codex workflow paths, task scaffolding, assistant-specific files, or retired
root doc paths. The separate maintainer-only `just codex-test` command is
intentionally outside the product release surface.

CI runs the product gate and broker-backed JetStream integration as separate
required jobs. CI also runs the release-surface guard. CI includes a
capability-gated FUSE smoke job: it runs the real smoke path when `/dev/fuse`
is present and records an explicit skip notice when the runner cannot mount
FUSE.
