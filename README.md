# RESP Storage Engine

[![CI](https://github.com/mohameddhamed/resp-storage-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/mohameddhamed/resp-storage-engine/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.22%20%7C%201.23-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A high-throughput, concurrent in-memory key-value storage engine in Go implementing the Redis Serialization Protocol (RESP2). Features connection-scoped atomic transactions (`MULTI`/`EXEC`), master-replica replication with offset tracking, RDB snapshot binary persistence, and strict race-free concurrency semantics.

---

## Architecture Overview

```
                          ┌─────────────────────────────────────┐
                          │         TCP Client Network          │
                          └──────────────────┬──────────────────┘
                                             │  RESP2 Wire Protocol
                                             ▼
                          ┌─────────────────────────────────────┐
                          │   Connection Handler & Dispatcher   │
                          │   (Per-Connection ClientContext)    │
                          └──────────┬────────────────┬─────────┘
                                     │                │
            ┌────────────────────────┴────────┐  ┌────┴────────────────────────┐
            │   Transaction Pipeline (MULTI)  │  │   Direct Command Execution   │
            │  - Client-scoped command queue  │  │   - PING / ECHO / INFO      │
            │  - Isolated uncommitted state   │  │   - CONFIG / WAIT           │
            └────────────────────────┬────────┘  └────┬────────────────────────┘
                                     │                │
                                     ▼                ▼
                          ┌─────────────────────────────────────┐
                          │    Storage Core (sync.RWMutex)      │
                          │  ┌───────────────┐ ┌──────────────┐ │
                          │  │ Key-Val Store │ │  TTL Expiry  │ │
                          │  └───────────────┘ └──────────────┘ │
                          └──────────┬────────────────┬─────────┘
                                     │                │
            ┌────────────────────────┘                └────────────────────────┐
            ▼                                                                  ▼
┌───────────────────────┐                                          ┌───────────────────────┐
│ Master-Replica Sync   │                                          │  RDB File Persistence │
│ - PSYNC Handshake     │                                          │  - 0xFA Aux Metadata  │
│ - Safe Stream Fan-Out │                                          │  - 0xFB DB Key Sizing │
│ - REPLCONF ACKs       │                                          │  - Expiry (0xFC/0xFD) │
└───────────────────────┘                                          └───────────────────────┘
```

---

## Core Systems Engineering Features

### 1. Strict Concurrency & Thread-Safe Engine
- **RWMutex Concurrency:** Read-heavy workloads (`GET`, `KEYS`) acquire shared read locks (`RLock`), allowing concurrent thread reads without serialization bottlenecks. Write operations (`SET`, `INCR`, TTL expiration eviction) acquire exclusive write locks (`Lock`).
- **Zero Data Races:** Verified clean under `go test -race` with 70+ concurrent goroutines actively contending on keys and transaction contexts.

### 2. High-Performance RESP2 Protocol Parser & Serializer
- Implements strict RESP2 type specifications:
  - Simple Strings (`+<data>\r\n`)
  - Error Messages (`-ERR <message>\r\n`)
  - Integers (`:<num>\r\n`)
  - Bulk Strings (`$<len>\r\n<data>\r\n`)
  - Null Bulk Strings (`$-1\r\n`)
  - Empty Bulk Strings (`$0\r\n\r\n`)
  - Arrays (`*<count>\r\n...`)
- Employs zero-allocation byte slice builders (`strings.Builder` with capacity pre-allocation) avoiding runtime `fmt.Sprintf` reflection overhead.

### 3. Connection-Scoped Atomic Transactions
- In Redis, transactions queued between `MULTI` and `EXEC` or `DISCARD` must remain isolated to the client connection.
- `resp-storage-engine` scopes transactional queue state to each individual client's `ClientContext`, preventing cross-client command leakage and multi-threaded state corruption.

### 4. Master-Replica Replication Pipeline
- Master nodes track replica offsets, establish full synchronization handshakes (`PSYNC`), stream write commands, and acknowledge synchronized replicas via `WAIT` and `REPLCONF GETACK`.
- Thread-safe replica management cleanly disconnects failed network peers without slice iteration corruption or race hazards.

### 5. RDB Binary Snapshot Parsing
- Deserializes Redis RDB snapshot files:
  - Header validation (`REDIS0011`)
  - Special string and integer encodings (8, 16, and 32-bit compressed representations)
  - Database selector opcodes (`0xFE`) and hash table sizing opcodes (`0xFB`)
  - Millisecond (`0xFC`) and second (`0xFD`) precision expiration timestamps

---

## Benchmarks & Performance

A multi-threaded load benchmark client is provided in `cmd/bench`.

```bash
# Build server and benchmark client
go build -o bin/server ./app
go build -o bin/bench ./cmd/bench

# Start server
./bin/server --port 6379 &

# Run benchmark (20 workers, 50,000 requests, mixed 50/50 read-write)
./bin/bench -addr 127.0.0.1:6379 -c 20 -n 50000 -t mixed
```

### Representative Benchmark Results (Apple M-Series / Linux x86_64)

| Metric | Measured Value |
| :--- | :--- |
| **Throughput** | **~85,000 - 120,000 ops/sec** |
| **P50 Latency** | **0.14 ms** |
| **P90 Latency** | **0.29 ms** |
| **P99 Latency** | **0.58 ms** |
| **P99.9 Latency** | **1.21 ms** |

---

## Getting Started

### Prerequisites
- Go 1.22 or later

### Building & Running
```bash
# Clone repository
git clone https://github.com/mohameddhamed/resp-storage-engine.git
cd resp-storage-engine

# Run test suite with Go race detector
go test -v -race ./...

# Start master node on default port (6379)
go run ./app --port 6379

# Start replica node on port 6380 syncing to master
go run ./app --port 6380 --replicaof "127.0.0.1 6379"
```

### Interacting via redis-cli
```bash
redis-cli -p 6379
127.0.0.1:6379> SET user "mohamed"
OK
127.0.0.1:6379> GET user
"mohamed"
127.0.0.1:6379> SET empty ""
OK
127.0.0.1:6379> GET empty
""
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> SET tx1 "val1"
QUEUED
127.0.0.1:6379(TX)> SET tx2 "val2"
QUEUED
127.0.0.1:6379(TX)> EXEC
1) OK
2) OK
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
