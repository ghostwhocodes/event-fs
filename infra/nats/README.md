Local NATS for EventFS
======================

This directory contains a simple `docker-compose.yml` to start a local NATS
server for development.

Requirements
------------

- Docker
- docker-compose (or `docker compose` with recent Docker versions)

Usage
-----

From the repository root:

```sh
cd infra/nats
docker compose up -d
```

This starts a NATS server listening on:

- `nats://127.0.0.1:4222` for clients
- `http://127.0.0.1:8222` for the NATS monitoring endpoint

To stop the server:

```sh
docker compose down
```
