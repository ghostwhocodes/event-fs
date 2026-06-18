# EventFS development tasks

test_threads := env_var_or_default("RUST_TEST_THREADS", "4")

default:
    @just --list

build:
    cargo build --workspace

build-release:
    cargo build --workspace --release

test:
    bash scripts/run_managed_command.sh env RUST_TEST_THREADS={{test_threads}} cargo test --workspace

test-one NAME:
    bash scripts/run_managed_command.sh env RUST_TEST_THREADS={{test_threads}} cargo test --workspace {{NAME}}

codex-test:
    python3 -m unittest discover -s codex/tests -p 'test_*.py'

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

check: fmt-check lint test

ci: check

coverage-full-summary:
    cargo llvm-cov --workspace --summary-only

check-logged LABEL='just-check':
    bash scripts/run_logged_command.sh {{LABEL}} just check

integration:
    docker compose -f infra/nats/docker-compose.yml down --volumes --remove-orphans
    docker compose -f infra/nats/docker-compose.yml up -d
    NATS_URL=${NATS_URL:-nats://127.0.0.1:4222} cargo test --workspace --features jetstream-tests

smoke:
    ./smoke-eventfs.sh

run-fuse *ARGS:
    cargo run -p eventfs-fuse -- {{ARGS}}

release-check:
    bash scripts/release-check.sh

release-check-strict:
    bash scripts/release-check.sh --strict

release-surface-guard:
    bash scripts/release-surface-guard.sh

clean:
    cargo clean
