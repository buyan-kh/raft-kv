# raft-kv

A replicated key-value store built on a from-scratch Raft implementation in Java 21. No consensus library: leader election, log replication, a write-ahead log, snapshots, and linearizable reads, served over HTTP.

## Features

- Leader election and log replication for 1 to 5 node clusters
- Durable write-ahead log with fsync, plus snapshots and InstallSnapshot
- Linearizable reads and exactly-once writes (clientId + seq dedup)
- HTTP API: PUT, GET, DELETE under /v1/kv/{key}
- Deterministic simulator with crashes, partitions, delay, and message loss
- Per-key linearizability checker run over every chaos history
- Web visualizer that replays recorded election, crash, partition, and snapshot traces

## Results

Measured on a 4-CPU Intel Xeon, Linux, Java 21, ext4. Raw data is in results/.

- 3 nodes, in memory: 315,838 puts/sec, p99 34.6 µs
- 3 nodes, fsync on every write: 11,374 puts/sec, p99 881 µs
- 3 nodes over HTTP with 4 clients: 5,528 ops/sec
- Chaos: 20 seeds on 5 nodes with 5% loss, crashes, and partitions gave 0 linearizability violations and 0 convergence failures

## Quick start

Requires Java 21.

```bash
./gradlew test
./gradlew run --args="--nodes 3 --port 7101 --data data --fsync true"
curl -X PUT localhost:7101/v1/kv/greeting -d '{"clientId":"c1","seq":1,"value":"hello"}'
curl localhost:7101/v1/kv/greeting
./gradlew chaos    # chaos campaign
./gradlew bench    # benchmarks
```

A 409 response means the node is not the leader; the body includes leaderId.

Visualizer:

```bash
cd web && npm ci && npm run dev
```

## Layout

```
src/main/java/raftkv/
  raft/      Raft node, log, and message types
  persist/   memory and file-backed log stores
  kv/        key-value state machine
  server/    HTTP server and per-node event loop
  sim/       deterministic cluster simulator and chaos runner
  linear/    linearizability checker
  bench/     benchmark harness
web/         Next.js trace viewer
docs/        design walkthrough
```

See docs/WALKTHROUGH.md for design notes.
