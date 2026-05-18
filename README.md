# opensender

A high-concurrency point-to-point file transfer tool for **high-latency links
where single TCP streams perform terribly but many parallel streams aggregate
real bandwidth** (e.g., long-haul Tailscale direct-connect).

Single Go binary, zero dependencies. One source tree builds both server (Linux)
and client (Windows) via cross-compilation.

## Why this exists

On one specific cross-border link, measured behavior was:

| Tool / config           | Throughput      |
|-------------------------|-----------------|
| `scp` (1 stream)        | ~47.9 KB/s      |
| `iperf3 -P 1`           | ~1 Mbps, unstable |
| `iperf3 -P 64`          | ~20.9 Mbps      |
| `iperf3 -P 128`         | ~51.5 Mbps      |

The bandwidth is there; single TCP just collapses. Existing tools have caps:
official `aria2` hard-limits `--max-connection-per-server` to 16, which leaves
most of the aggregate ceiling on the table.

opensender is built to push concurrency arbitrarily high (128 / 256 / 512+),
with per-chunk SHA256 validation, resume, and recursive directory transfer.

## Build

```sh
# Native (Linux server side)
go build -o opensender ./...

# Cross-compile Windows client
GOOS=windows GOARCH=amd64 go build -o opensender.exe ./...
```

Requires Go ≥ 1.22. No external modules.

## Use

### Server (Linux)

```sh
opensender serve --root /data/models --token YOUR_TOKEN --listen :8080
```

Serves files under `--root` over HTTP. Bearer-token auth. No TLS — intended to
run behind something that already encrypts (Tailscale, WireGuard, SSH tunnel).

### Client (Windows or any OS)

```sh
opensender pull \
    --url    http://<server-ip>:8080 \
    --remote checkpoints/ \
    --local  D:\models\ \
    --token  YOUR_TOKEN \
    --concurrency 128 \
    --chunk 4M
```

`--remote` is a path relative to the server's `--root`; empty string means the
whole root. The client recurses into subdirectories.

If interrupted, just rerun the same command — a `.opensender-manifest.json`
sidecar file in `--local` tracks which chunks finished, and the next run
fetches only the missing ones.

### Flags (`pull`)

| Flag              | Default | Notes                                  |
|-------------------|---------|----------------------------------------|
| `--concurrency`   | 64      | Number of parallel chunk workers.      |
| `--chunk`         | 4M      | Chunk size. K / M / G suffixes ok.     |
| `--retries`       | 5       | Per-chunk retry budget.                |
| `--chunk-timeout` | 5m      | Per-request timeout.                   |

## How it works

- Server exposes `GET /list?path=...` (JSON of files+sizes) and `GET /file/<path>`
  with HTTP `Range` headers. Every range response carries an `X-Chunk-SHA256`
  header with the SHA-256 of the bytes about to be sent.
- Client lists the tree, pre-allocates output files via `Truncate`, and builds
  one **global** chunk queue mixing chunks from every file. N workers each hold
  their own TCP connection (HTTP/1.1 forced; HTTP/2 is explicitly disabled
  because it multiplexes onto a single TCP, defeating the design goal).
- Each worker reads a chunk, verifies SHA-256, and uses `WriteAt` to place it at
  the right offset (atomic positional write on both POSIX and Windows).
- A manifest goroutine flushes completed-chunk indices to disk every 2 s, so a
  killed run loses at most ~2 s of progress.

## Status

v0.1, written in one session. Smoke-tested on loopback (correctness, resume,
high concurrency, manifest deletion / partial removal). Real-link benchmarks
TBD — see `MEMORY.md` history if you want the design discussion.

## License

MIT.
