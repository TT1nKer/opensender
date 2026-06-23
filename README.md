# opensender

A high-concurrency point-to-point file transfer tool for **high-latency links
where single TCP streams perform terribly but many parallel streams aggregate
real bandwidth** (e.g., long-haul Tailscale direct-connect).

Single Go binary, zero dependencies. One source tree builds both server (Linux)
and client (Windows) via cross-compilation.

## Why this exists

On the specific cross-border link this tool was built for, measured behavior:

| Tool / config     | Throughput          |
|-------------------|---------------------|
| `scp` (1 stream)  | ~47.9 KB/s          |
| `iperf3 -P 1`     | ~1 Mbps, unstable   |
| `iperf3 -P 64`    | ~20.9 Mbps          |
| `iperf3 -P 128`   | ~51.5 Mbps          |
| **opensender, c=1024** | **~7.5 MiB/s (≈ 60 Mbps)** |

The bandwidth is there; single TCP just collapses. Existing tools have caps:
official `aria2` hard-limits `--max-connection-per-server` to 16, which leaves
most of the aggregate ceiling on the table. opensender pushes concurrency
arbitrarily high (default 1024), with per-chunk SHA-256 validation, request
hedging to kill the long-tail effect, manifest-based resume, and recursive
directory transfer.

## Real-world benchmark (this link, 500 MiB random file)

Tested 2026-05-18 over Tailscale (Canada ↔ China), pulling to a Windows
client behind a 100 Mbps residential LAN.

| Concurrency | Chunk | Throughput | Notes |
|-------------|-------|------------|-------|
| 1024        | 256K  | **7.58 MiB/s** | sweet spot |
| 2048        | 256K  | 7.15 MiB/s | hedging keeps it competitive |
| 4096        | 256K  | 5.41 MiB/s | hedging bandwidth cost outweighs gain |

**vs `scp` baseline of 47.9 KB/s ≈ ~170× speedup at the sweet spot.**

Observed: at peak instantaneous rate the transfer hits ~9 MiB/s (~72 Mbps),
near the 100 Mbps LAN ceiling. The cross-border link itself may have more
headroom that the client-side LAN can't sink.

## Build

```sh
# Native (e.g. Linux server side)
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

Serves files under `--root` over HTTP with bearer-token auth. No TLS —
intended to run behind something that already encrypts (Tailscale, WireGuard,
SSH tunnel).

### Client — first-time setup

Run once to save your server URL, token, and default local dir to
`~/.opensender.json` (Windows: `%USERPROFILE%\.opensender.json`, mode 0600):

```sh
opensender init
```

### Client — daily use

```sh
opensender pull --remote checkpoints/sd_xl.safetensors
opensender pull --remote vae/                              # recursive
opensender pull --remote "" --include "*.pdf"              # matching files
opensender pull --remote checkpoints/foo.bin --local E:\elsewhere\
```

`--remote` is a path relative to the server's `--root`. Empty means the
whole root. The client recurses into subdirectories.

Use `--include` to filter the listed files with a glob. The pattern is matched
against both the remote relative path and the base filename, so `*.pdf` matches
PDF files anywhere under the selected `--remote` directory.

If interrupted, just rerun the same command — a `.opensender-manifest.json`
sidecar in `--local` records which chunks finished, and the next run
fetches only the missing ones.

### Full flag form (no config)

```sh
opensender pull \
    --url    http://<server-ip>:8080 \
    --remote checkpoints/ \
    --local  D:\models\ \
    --token  YOUR_TOKEN
```

### Per-run overrides

| Flag              | Default | Notes                                                |
|-------------------|---------|------------------------------------------------------|
| `--concurrency`   | 1024    | Parallel chunk workers.                              |
| `--chunk`         | 256K    | K / M / G suffixes ok.                               |
| `--hedge-after`   | 3s      | Tail-phase re-issue threshold. 0 disables hedging.   |
| `--chunk-timeout` | 5m      | Per-request timeout.                                 |
| `--retries`       | 5       | Per-chunk retry budget before permanent failure.     |
| `--include`       | unset   | Optional glob filter, e.g. `*.pdf`.                  |

Defaults are tuned from real-world benchmarks on the target link. Other
links may want different values; if so, edit `~/.opensender.json` to make
them sticky.

## How it works

- Server exposes `GET /list?path=...` (JSON of files + sizes) and
  `GET /file/<path>` with HTTP `Range` headers. Every range response carries
  an `X-Chunk-SHA256` header with the SHA-256 of the bytes about to be sent.
- Client lists the tree, pre-allocates output files via `Truncate`, and
  builds one **global** chunk queue mixing chunks from every file. N workers
  each hold their own TCP connection (HTTP/1.1 forced; HTTP/2 is explicitly
  disabled because it multiplexes onto a single TCP, defeating the design).
- Each worker reads a chunk, verifies SHA-256, and uses `WriteAt` to place
  it at the right offset (atomic positional write on both POSIX and Windows).
- **Hedging**: during the tail phase (`remaining < concurrency/4`), a
  watchdog re-issues in-flight chunks older than `--hedge-after` to idle
  workers. First completion wins via `sync.Map.LoadAndDelete`; losers
  discard their buffer. Capped at 2 speculations per chunk to bound waste
  on bandwidth-limited links.
- A manifest goroutine flushes completed-chunk indices to disk every 2s, so
  a killed run loses at most ~2s of progress.
- When the last chunk is claimed, a context cancellation aborts any
  in-flight HTTP reads on slow connections immediately — the process exits
  the moment progress hits 100%, not minutes later.

## Status

v0.1.x, single maintainer use. Smoke-tested on loopback (correctness,
resume, hedging path, partial manifest), real-link benchmarked over
Tailscale cross-border. The flow that got us here is in the project memory
(`~/.claude/projects/-home-hostsjim-Projects-opensender/memory/`) if you
want the design history.

## License

MIT.
