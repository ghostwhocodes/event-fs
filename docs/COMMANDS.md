# Commands

## FUSE Mount

```text
eventfs-fuse <MOUNTPOINT> [NATS_URL]
  --nats-creds-file <PATH>       NATS credentials file. Also read from NATS_CREDS_FILE.
  --mount-name <NAME>            Stable name shown in diagnostics. Default: eventfs.
  --cache-dir <DIR>              Durable cache and writeback queue directory.
  --timeout-ms <N>               Per-operation JetStream timeout. Default: 2000.
  --duplicate-window-ms <N>      Stream duplicate window. Default: 86400000.
  --queue-capacity <N>           Maximum durable writeback queue entries. Default: 1024.
  --foreground                   Accepted by the CLI; the current mount process already blocks in the foreground.
```

`NATS_URL` defaults to `nats://127.0.0.1:4222`.

Run through Cargo:

```sh
cargo run -p eventfs-fuse -- /tmp/eventfs nats://127.0.0.1:4222
```

Run through the helper:

```sh
./run-fuse.sh /tmp/eventfs nats://127.0.0.1:4222 --mount-name dev
```

`run-fuse.sh` strips credentials from the URL when `NATS_CREDS_FILE` is set and
passes the credentials file to the mount process through the environment.

## Probe

`eventfs-probe` verifies the broker state created by the smoke script.

```sh
cargo run -p eventfs-transport --bin eventfs-probe -- nats://127.0.0.1:4222
```

It checks the smoke KV record, stream messages, object, and task record.

## Scripts

```sh
./run-fuse.sh <mountpoint> [nats-url] [eventfs-fuse args...]
./smoke-eventfs.sh
./smoke.sh
scripts/release-check.sh [--local|--strict]
scripts/release-surface-guard.sh
```

- `run-fuse.sh` starts the FUSE mount.
- `smoke-eventfs.sh` starts a real mount, writes through representative
  roots, verifies broker state independently, and unmounts.
- `smoke.sh` delegates to `smoke-eventfs.sh`.
- `scripts/release-check.sh` runs the repository release validation flow. Local
  mode is the default and reports Docker or FUSE skips explicitly. If local
  broker setup is skipped, FUSE smoke also requires an explicit `NATS_URL`.
  Strict mode fails when broker-backed integration or FUSE smoke prerequisites
  are missing.
- `scripts/release-surface-guard.sh` verifies that product release commands
  and product-facing docs do not depend on Codex workflow paths, task
  scaffolding, assistant-specific files, or retired root doc paths.

## Just Recipes

```sh
just build
just build-release
just test
just test-one <name>
just codex-test
just lint
just fmt
just fmt-check
just check
just integration
just smoke
just run-fuse <mountpoint> [nats-url] [args...]
just release-check
just release-check-strict
just release-surface-guard
just clean
```

`just check` is the product gate. It runs format check, clippy with warnings
denied, and workspace Rust tests without requiring repo-local Codex workflow
files. `just codex-test` is a maintainer-only source-repository check for the
Codex long-horizon workflow runtime. `just integration` resets the local NATS
Compose service, starts a clean broker, and runs broker-backed tests with
`jetstream-tests`.
