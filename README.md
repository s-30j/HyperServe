# HyperServe

High-performance Node.js download server for VPS deployment — streams large files (game maps, mods, installers) directly from disk with clustering, HTTPS, rate limiting, and crash-resistant memory management for high-traffic hosting.

## Overview

A robust, production-ready Node.js web server designed to run on a VPS and serve as a high-performance download hosting platform. Built specifically for distributing large files — game maps, mods, installers, and other bulky assets — reliably and at high speed, even under heavy simultaneous traffic.

Instead of loading files into memory before sending them (which crashes servers under load), this project streams files directly from disk to the client, keeping memory usage flat regardless of file size or number of concurrent downloads. It uses Node's `cluster` module to spread traffic evenly across all available CPU cores, so no single core becomes a bottleneck while others sit idle.

## Features

- **Streaming-first architecture** — files above a configurable size threshold are never fully buffered in memory; they stream straight from disk to the client
- **Multi-core clustering** — automatically spawns one worker per CPU core with round-robin load balancing
- **HTTPS/SSL support** — works with Let's Encrypt or custom `.pem` certificates, with automatic HTTP → HTTPS redirection
- **Resumable downloads** — full HTTP range request support (pause/resume, download managers)
- **Per-IP rate limiting** — configurable request limits with automatic cleanup to prevent memory leaks
- **Two-tier caching** — optional Redis-backed shared cache (L2) plus in-memory per-worker cache (L1) for frequently requested small files
- **Directory listing** — browsable file index for the public directory, with optional subdirectory access restrictions
- **OOM protection** — per-worker memory budgeting, capped V8 heap size, and an in-flight buffering byte budget so bursts of concurrent downloads can't exhaust RAM
- **Load shedding** — a configurable cap on concurrent streams per worker; requests beyond the cap get an immediate `503 Retry-After` instead of piling up
- **Crash-resistant** — client disconnects, aborted downloads, and socket errors are handled gracefully instead of crashing the worker; workers that do crash restart automatically with backoff
- **Security headers** — sane defaults for `Content-Disposition`, `X-Content-Type-Options`, and path traversal protection

## Requirements

- Node.js 16+
- A VPS or dedicated server (Linux recommended)
- (Optional) Redis, if you want shared caching across workers
- SSL certificate — Let's Encrypt (Certbot) or your own `.pem` files, if serving over HTTPS

## Installation

```bash
git clone https://github.com/s-30j/HyperServe.git
cd HyperServe
npm install
```

edit environment file for your setup:

```bash
nano .env
```

Place the files you want to serve inside your `PUBLIC_DIR` (default: `public/`).

Start the server:

```bash
node server.js
```

For production, run it under a process manager so it restarts on reboot (see [Running as a systemd service](#running-as-a-systemd-service) below).

## Configuration

All configuration is done through environment variables (`.env` file).

### Server

| Variable | Default | Description |
|---|---|---|
| `DOMAIN` | `localhost` | Your domain name (used for logging/startup info) |
| `PORT` | `443` | HTTPS port |
| `HTTP_PORT` | `80` | HTTP port (redirects to HTTPS) |
| `SSL_KEY` | `ssl/privkey.pem` | Path to SSL private key |
| `SSL_CERT` | `ssl/fullchain.pem` | Path to SSL certificate |
| `PUBLIC_DIR` | `public` | Directory containing files to serve |
| `ALLOWED_SUBDIRS` | *(empty = all allowed)* | Comma-separated list of subdirectories allowed to be served |
| `NUM_WORKERS` | `0` (auto) | Number of cluster workers; `0` uses all available CPU cores |

### Cache / Performance

| Variable | Default | Description |
|---|---|---|
| `STREAM_HIGH_WATER_MARK` | `1048576` (1MB) | Stream buffer size in bytes |
| `MEMORY_CACHE_MAX_SIZE` | `1073741824` (1GB) | Max in-memory (L1) cache size per worker; auto-capped based on available system RAM |
| `REDIS_CACHE_MAX_SIZE` | `104857600` (100MB) | Max file size eligible for Redis (L2) caching |
| `REDIS_URL` | *(empty = disabled)* | Redis connection URL, e.g. `redis://localhost:6379`. Leave empty to use RAM cache only |

### Security

| Variable | Default | Description |
|---|---|---|
| `MAX_FILE_SIZE` | `0` (unlimited) | Max servable file size in bytes |
| `RATE_LIMIT` | `200` | Max requests per IP per rate window |
| `RATE_WINDOW` | `20000` (20s) | Rate limit window in milliseconds |

### Stability / OOM Protection

| Variable | Default | Description |
|---|---|---|
| `MAX_CACHEABLE_FILE_SIZE` | `26214400` (25MB) | Files larger than this are **never** buffered in RAM — they always stream directly from disk. Keep this low; large files (maps/mods) should stream, not cache |
| `MAX_CONCURRENT_STREAMS` | `80` | Max simultaneous downloads served per worker. Requests beyond this get an immediate `503` with `Retry-After` instead of risking an OOM kill |

## How it works

- On startup, the **master process** forks one **worker** per CPU core (configurable via `NUM_WORKERS`) and load-balances incoming connections across them round-robin.
- Small, frequently requested files (below `MAX_CACHEABLE_FILE_SIZE`) are cached in memory (and optionally in Redis) to serve them instantly without touching disk.
- Large files are always streamed directly from disk using Node's `fs.createReadStream`, so memory usage stays flat no matter how big the file is or how many people download it at once.
- If a worker is already serving `MAX_CONCURRENT_STREAMS` downloads, new requests get a `503` response so the client can retry — rather than the worker accepting unlimited work and risking a crash.
- If a worker does crash for any reason, the master process automatically spawns a replacement with exponential backoff.

