# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**trcache** is a C library for ingesting real-time trade data and transforming it into column-oriented, user-defined candle arrays. It uses lock-free data structures and a multi-threaded worker pool to scale with CPU cores.

## Build Commands

### Linux (Makefile)

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

### Windows (CMake)

```powershell
# MSVC 64-bit
cmake -B build -A x64
cmake --build build --config Release    # or Debug

# MinGW
cmake -B build -G "MinGW Makefiles"
cmake --build build
```

CMake debug mode: `-DCMAKE_BUILD_TYPE=Debug` (MinGW) or `--config Debug` (MSVC).

Note: CMake builds the static library only. Benchmarks and validator are Makefile-only targets.

### Build Outputs

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

## Code Style

- 80-character line limit (code files; docs are exempt).
- Always use braces for `if`/`else`/`for`/`while`, even single-line bodies.
- Include paths: `-I$(PROJECT_ROOT) -I$(PROJECT_ROOT)/src/include`. Internal headers live under `src/include/<module>/`.
- Cross-platform compatibility headers live in `src/include/compat/` (builtins, aligned memory, threads). Use these instead of platform-specific APIs directly.
- Trade pipeline uses `candle` terminology; book pipeline uses `snapshot`/`record`.

## Testing

There is no unit test suite. Correctness is validated via:
- **Benchmarks** (`benchmark/`) — stress-test throughput and read latency.
- **Live validator** (`validator/`) — feeds real Binance Futures data and checks for gaps/tick errors.

## Architecture

### Pipeline Stages

Two independent pipelines share the same worker pool:

**Trade pipeline** (candle aggregation):
```
trcache_feed_trade_data()
  → event_data_buffer  (per-symbol SPMC queue)
  → [Apply]   worker threads aggregate trades into row-oriented candles (AoS)
  → [Convert] worker threads transform complete rows into SIMD-aligned column batches (SoA)
  → [Flush]   worker threads pass batches to user callbacks
```

**Book pipeline** (order book state, optional):
```
trcache_feed_book_data()
  → event_data_buffer  (per-symbol SPMC queue)
  → [Apply]   worker threads call book_state_ops.update() to maintain book state
  → [Flush]   worker threads pass completed event blocks to book_event_flush_ops
```

Book state is passed to candle `init`/`update` callbacks, enabling book-aware candle construction.

Query APIs read from immutable column batches lock-free, concurrently with writes.

### Key Source Modules

| Module | Path | Role |
|--------|------|------|
| Public API | `trcache.h` | All user-facing types and function declarations |
| Core init | `src/meta/trcache_internal.c` | Initialization, memory validation, shutdown |
| Candle chunk | `src/pipeline/candle_chunk.c` | Row-page storage; tracks mutable vs. immutable rows |
| Chunk list | `src/pipeline/candle_chunk_list.c` | Per-(symbol, candle_type) linked list; bulk row→column conversion |
| Chunk index | `src/pipeline/candle_chunk_index.c` | Fast lookup from (symbol_id, candle_type) to chunk list |
| Event buffer | `src/pipeline/event_data_buffer.c` | Fixed-size event blocks buffered per symbol |
| Worker threads | `src/sched/worker_thread.c` | Task execution via 64-bit bitmap CAS scan |
| Admin thread | `src/sched/admin_thread.c` | EMA-based throughput monitoring; dynamic worker rebalancing |
| Atomic snapshot | `src/concurrent/atomsnap.c` | Versioned atomic pointer for lock-free reads |
| Scalable queue | `src/concurrent/scalable_queue.c` | MPMC queue with per-thread TLS; used for object pooling |
| Hash table | `src/utils/hash_table.c` | Open-addressing hash table; used by symbol table |

### Concurrency Model

- **Workers** are divided into two groups: In-Memory (Apply + Convert + Book Update) and Flush (Batch-Flush + Trade-Flush + Book-Event-Flush). The admin thread dynamically adjusts group sizes based on EMA of per-stage cycle costs.
- **atomsnap** provides the versioned atomic snapshot abstraction used for lock-free reads of row pages and column batches.
- **scalable_queue** is the MPMC pool with per-thread TLS free lists to minimize contention.
- Memory pressure is tracked via a global atomic flag; when high, objects are freed rather than pooled.
- Only TLS initialization/destruction uses a mutex; all data-path operations are lock-free.

### Storage Layout

- Rows (Apply stage): Array-of-Structs (AoS), 64 rows per page (`TRCACHE_ROWS_PER_PAGE = 64`).
- Columns (after Convert): Struct-of-Arrays (SoA), SIMD-aligned to 64 bytes (`TRCACHE_SIMD_ALIGN = 64`).
- Two atomic snapshots per chunk: one for row pages, one for column batches.

### Validator (C++)

Located in `validator/`, built with g++ C++17. Connects to Binance Futures WebSocket, feeds live trades into the engine, and checks data integrity (gap and tick-error detection). Configuration is driven by `validator/config.json`.

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
