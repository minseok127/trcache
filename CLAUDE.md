# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**trcache** is a C library for ingesting real-time trade data and transforming it into column-oriented, user-defined candle arrays. It uses lock-free data structures and a multi-threaded worker pool to scale with CPU cores.

## Build Commands

```bash
# Build library (release, default)
make

# Build library (debug mode — adds -g, -DTRCACHE_DEBUG, disables optimization)
make BUILD_MODE=debug

# Clean all artifacts
make clean

# Build benchmark executables (always release mode)
make benchmark

# Build validator app (requires simdjson, openssl, libcurl)
make -C validator all
```

Build outputs: `libtrcache.a` and `libtrcache.so` in the project root.

Benchmark executables (`feed_only_benchmark`, `static_read_benchmark`, `htap_benchmark`) are built in `benchmark/`.

The validator (`validator_app`) is built in `validator/` and requires external C++ dependencies: `simdjson`, `openssl`, `libcurl`.

## Running Benchmarks and Validator

```bash
# Write-heavy stress test (1024 symbols, Zipf-distributed)
./benchmark/feed_only_benchmark

# Read-only latency test on static data
./benchmark/static_read_benchmark

# Mixed OLTP/OLAP concurrent benchmark
./benchmark/htap_benchmark

# Live Binance Futures validation (requires network)
cd validator && ./validator_app config.json
```

## Architecture

### Pipeline Stages

Trades flow through a 3-stage lock-free pipeline:

```
trcache_feed_trade_data()
  → trade_data_buffer  (per-symbol MPMC queue)
  → [Apply]   worker threads aggregate trades into row-oriented candles (AoS)
  → [Convert] worker threads transform complete rows into SIMD-aligned column batches (SoA)
  → [Flush]   worker threads pass batches to user callbacks
```

Query APIs read from immutable column batches lock-free, concurrently with writes.

### Key Source Modules

| Module | Path | Role |
|--------|------|------|
| Public API | `trcache.h` | All user-facing types and function declarations |
| Core init | `src/meta/trcache_internal.c` | Initialization, memory validation, shutdown |
| Candle chunk | `src/pipeline/candle_chunk.c` | Row-page storage; tracks mutable vs. immutable rows |
| Chunk list | `src/pipeline/candle_chunk_list.c` | Per-(symbol, candle_type) linked list; bulk row→column conversion |
| Chunk index | `src/pipeline/candle_chunk_index.c` | Fast lookup from (symbol_id, candle_type) to chunk list |
| Trade buffer | `src/pipeline/trade_data_buffer.c` | Fixed-size trade chunks buffered per symbol |
| Worker threads | `src/sched/worker_thread.c` | Task execution via 64-bit bitmap CAS scan |
| Admin thread | `src/sched/admin_thread.c` | EMA-based throughput monitoring; dynamic worker rebalancing |
| Atomic snapshot | `src/concurrent/atomsnap.c` | Versioned atomic pointer for lock-free reads |
| Scalable queue | `src/concurrent/scalable_queue.c` | MPMC queue with per-thread TLS; used for object pooling |

### Concurrency Model

- **Workers** are divided into three groups: In-Memory (Apply + Convert), Batch-Flush, and Trade-Flush. The admin thread dynamically adjusts group sizes based on EMA of per-stage cycle costs.
- **atomsnap** provides the versioned atomic snapshot abstraction used for lock-free reads of row pages and column batches.
- **scalable_queue** is the MPMC pool with per-thread TLS free lists to minimize contention.
- Memory pressure is tracked via a global atomic flag; when high, objects are freed rather than pooled.
- Only TLS initialization/destruction uses a mutex; all data-path operations are lock-free.

### Storage Layout

- Rows (Apply stage): Array-of-Structs (AoS), 64 rows per page (`TRCACHE_ROWS_PER_PAGE = 64`).
- Columns (after Convert): Struct-of-Arrays (SoA), SIMD-aligned to 64 bytes (`TRCACHE_SIMD_ALIGN = 64`).
- Two atomic snapshots per chunk: one for row pages, one for column batches.

### Validator (C++)

Located in `validator/`, built with g++ C++17. Connects to Binance Futures WebSocket, feeds live trades into the engine, and checks data integrity (gap and tick-error detection). Results are appended to CSV files in `validator/results/`. Configuration is driven by `validator/config.json`.

## Important Constants (not in trcache.h)

```c
// candle_chunk.h
TRCACHE_ROWS_PER_PAGE       64   // rows per page (power of 2)
TRCACHE_ROWS_PER_PAGE_SHIFT  6   // log2(TRCACHE_ROWS_PER_PAGE)
```

```c
// trcache.h
MAX_NUM_THREADS    1024
MAX_CANDLE_TYPES     32
TRCACHE_SIMD_ALIGN   64  // bytes, for column batch alignment
CACHE_LINE_SIZE      64
```

## Debug Mode

Build with `BUILD_MODE=debug` to enable `-DTRCACHE_DEBUG` which activates internal assertions and disables optimization. The validator has no debug mode flag; adjust `CXXFLAGS` manually in `validator/Makefile` if needed.
