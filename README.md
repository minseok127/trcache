# TRCACHE

`trcache` is a C library for ingesting real-time trade data and transforming it into **column-oriented, user-defined candle arrays** optimized for analytics. Designed for multicore systems, it leverages lock-free data structures and dedicated worker threads to scale with available CPU resources.

---

## Features

- **Fully Customizable Candles**: Define any candle structure you need—time-based, tick-based, or custom aggregations. Not limited to OHLCV.
- **Lock-Free 3-Stage Pipeline**: 
  - **Apply**: Aggregates raw trades into row-oriented candles
  - **Convert**: Reshapes rows into SIMD-friendly column batches
  - **Flush**: Hands off completed batches to user callbacks for persistence
- **Lock-Free Queries**: Read candle data without locks, even during concurrent updates
- **Adaptive Scheduling**: Admin thread monitors pipeline throughput and dynamically balances worker threads across stages

---

## Performance Characteristics

*For detailed benchmark results and in-depth analysis, please see the Benchmark & Analysis section below.*

### Throughput (1 feed thread, 3 worker threads, 1024 symbols, Zipf s=0.99, 5GB memory limit)

- **Feed Rate**: `15,000,000` trades/sec (no concurrent readers)

### Query Latency (10,000 candles, offset-based, 1 reader, fields=3):

- **Static Read** (OLAP-only, no concurrent writes):
  - P50: `6.2` μs
  - P99: `9.18` μs
  - Mean: `6.4` μs

- **Concurrent Read** (HTAP, concurrent feed rate: `15,000,000` trades/sec):
  - P50: `27` μs
  - P99: `58` μs
  - Mean: `28` μs

### Scalability (1024 symbols, Zipf s=0.99, 1 reader, fields=3, 5GB memory limit)
- **num_worker_threads=3**

| # of Feed Threads | Feed Rate (trades / sec) | Query Latency (Mean, μs) | Query Latency (p50, μs) | Query Latency (p99, μs) |
|:-----------------:|:------------------------:|:------------------------:|:-----------------------:|:-----------------------:|
||||||

- **num_worker_threads=6**

| # of Feed Threads | Feed Rate (trades / sec) | Query Latency (Mean, μs) | Query Latency (p50, μs) | Query Latency (p99, μs) |
|:-----------------:|:------------------------:|:------------------------:|:-----------------------:|:-----------------------:|
||||||

---

## Limitations

- Concurrent feeds to same symbol not supported; a single thread can handle feeding data for multiple symbols.
- The `max_symbols` capacity is pre-allocated at initialization and cannot be changed at runtime.
- The `total_memory_limit` is a hard cap; `trcache_feed_trade_data` will return -1 if this limit is exceeded.
- Initialization will fail if `total_memory_limit` is set lower than the minimum memory required by the configuration.
- The number of worker threads (`num_worker_threads`) must be greater than 2 (minimum 3).
- `trcache_candle_base` must be the first member of any custom candle struct definition.
- The system has hard-coded compile-time limits, such as `MAX_CANDLE_TYPES` (32) and `MAX_NUM_THREADS` (1024).

---

## Build

```bash
make                    # build release mode (O2), same with BUILD_MODE=release
make BUILD_MODE=debug   # build with debug symbols and assertions
```

---

## Quick Start

Here's a minimal example to get `trcache` running in under 5 minutes:

```c
#include "trcache.h"

// 1. Define your candle structure (must start with trcache_candle_base)
typedef struct {
    trcache_candle_base base;  // REQUIRED as first member
    double open, high, low, close, volume;
} MyCandle;

// 2. Define field layout
const trcache_field_def fields[] = {
    {offsetof(MyCandle, open),   sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyCandle, high),   sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyCandle, low),    sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyCandle, close),  sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyCandle, volume), sizeof(double), FIELD_TYPE_DOUBLE},
};

// 3. Implement update callbacks
void init_1min_candle(trcache_candle_base *c, trcache_trade_data *d) {
    MyCandle *candle = (MyCandle *)c;
    c->key.timestamp = d->timestamp - (d->timestamp % 60000);  // 1-min window
    c->is_closed = false;
    candle->open = candle->high = candle->low = candle->close = d->price.as_double;
    candle->volume = d->volume.as_double;
}

bool update_1min_candle(trcache_candle_base *c, trcache_trade_data *d) {
    if (d->timestamp >= c->key.timestamp + 60000) {
        c->is_closed = true;
        return false;  // Start new candle
    }
    MyCandle *candle = (MyCandle *)c;
    double price = d->price.as_double;
    if (price > candle->high) candle->high = price;
    if (price < candle->low) candle->low = price;
    candle->close = price;
    candle->volume += d->volume.as_double;
    return true;
}

// 4. Initialize trcache
trcache_candle_config config = {
    .user_candle_size = sizeof(MyCandle),
    .field_definitions = fields,
    .num_fields = 5,
    .update_ops = {.init = init_1min_candle, .update = update_1min_candle},
    .flush_ops = {.flush = NULL}  // No-op flush for this example
};

trcache_init_ctx ctx = {
    .candle_configs = &config,
    .num_candle_configs = 1,
    .batch_candle_count_pow2 = 10,      // 1024 candles per batch
    .cached_batch_count_pow2 = 3,       // Cache 8 batches before flush
    .total_memory_limit = 5ULL << 30,   // 5GB
    .num_worker_threads = 4,
    .max_symbols = 1024
};

trcache *cache = trcache_init(&ctx);

// 5. Register symbol and feed data
int btc_id = trcache_register_symbol(cache, "BTC-USD");
trcache_trade_data trade = {
    .timestamp = 1609459200000,  // 2021-01-01 00:00:00 UTC
    .trade_id = 1,
    .price = {.as_double = 29000.0},
    .volume = {.as_double = 1.5}
};
trcache_feed_trade_data(cache, &trade, btc_id);

// 6. Query candles
int field_indices[] = {1, 2, 3};  // high, low, close
trcache_field_request request = {.field_indices = field_indices, .num_fields = 3};
trcache_candle_batch *batch = trcache_batch_alloc_on_heap(cache, 0, 100, &request);

trcache_get_candles_by_symbol_id_and_offset(cache, btc_id, 0, &request, 0, 10, batch);

double *highs = (double *)batch->column_arrays[1];   // Use original field index!
printf("Latest high: %.2f\n", highs[batch->num_candles - 1]);

trcache_batch_free(batch);
trcache_destroy(cache);
```

---

## Architecture

### Pipeline Overview

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│    APPLY     │─────▶│   CONVERT    │─────▶│    FLUSH     │
│  (Row AoS)   │       │ (Column SoA) │       │  (Persist)   │
└──────────────┘       └──────────────┘       └──────────────┘
       │                     │                      │
   Trade Data          Immutable Rows        Completed Batches
   Aggregation         → SIMD Columns        → User Callbacks
```

1. **APPLY**: Worker threads consume trades from lock-free buffers and update row-oriented candles. Only the most recent candle is mutable; all prior candles are immutable.

2. **CONVERT**: Once a candle completes, a worker transforms the immutable row (Array of Structs) into a column-oriented batch (Struct of Arrays) aligned for SIMD operations.

3. **FLUSH**: When the number of unflushed batches exceeds a threshold, a worker invokes user-provided callbacks to persist data (supports both sync and async I/O).

### Concurrency Model

- **Lock-Free Reads**: Queries use `atomsnap` (RCU-like snapshots) to read stable data without blocking writers
- **Memory Reclamation**: Grace-period mechanism ensures chunks are freed only after all readers finish
- **Object Pooling**: `scalable_queue` provides per-thread memory pools with pressure-aware recycling
- **Adaptive Scheduling**: Admin thread calculates EMA cycle costs per stage and dynamically assigns workers to balance load

---

## Usage Guide

### Step 1: Define Your Candle Structure

Your candle struct **must** have `trcache_candle_base` as its first member:

```c
typedef struct {
    trcache_candle_base base;  // REQUIRED: provides key and is_closed
    double open;
    double high;
    double low;
    double close;
    uint64_t volume;
    uint32_t trade_count;      // This field won't be in column arrays
} MyOHLCV;
```

### Step 2: Describe the Memory Layout

Create an array of `trcache_field_def` to map your custom fields:

```c
const trcache_field_def my_fields[] = {
    {offsetof(MyOHLCV, open),   sizeof(double),   FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, high),   sizeof(double),   FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, low),    sizeof(double),   FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, close),  sizeof(double),   FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, volume), sizeof(uint64_t), FIELD_TYPE_UINT64},
    // Note: trade_count is omitted - it's scratch space not stored in batches
};
```

### Step 3: Implement Candle Update Logic

```c
void init_tick(trcache_candle_base *c, trcache_trade_data *d) {
    MyOHLCV *candle = (MyOHLCV *)c;
    c->key.trade_id = d->trade_id;  // Use trade_id for tick candles
    c->is_closed = false;
    
    double price = d->price.as_double;
    candle->open = candle->high = candle->low = candle->close = price;
    candle->volume = d->volume.as_double;
    candle->trade_count = 1;
}

bool update_tick(trcache_candle_base *c, trcache_trade_data *d) {
    MyOHLCV *candle = (MyOHLCV *)c;
    double price = d->price.as_double;
    
    if (price > candle->high) candle->high = price;
    if (price < candle->low) candle->low = price;
    candle->close = price;
    candle->volume += d->volume.as_double;
    candle->trade_count++;
    
    if (candle->trade_count >= 100) {  // 100-tick candle
        c->is_closed = true;
        return false;  // Trigger new candle creation
    }
    return true;  // Trade consumed
}
```

### Step 4: Implement Flush Callbacks

#### Synchronous Example
```c
void* sync_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
    FILE *fp = (FILE *)ctx;
    fwrite(batch->key_array, sizeof(uint64_t), batch->num_candles, fp);
    // ... write other columns ...
    return NULL;  // NULL = synchronous completion
}

trcache_batch_flush_ops flush_ops = {.flush = sync_flush, .flush_ctx = my_file};
```

#### Asynchronous Example (io_uring)
```c
typedef struct {
    struct io_uring *ring;
    int fd;
} UringCtx;

void* async_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
    UringCtx *uring = (UringCtx *)ctx;
    
    // Allocate job context
    struct job {
        struct iovec iov;
        bool done;
    } *job = malloc(sizeof(*job));
    
    job->iov.iov_base = batch->key_array;
    job->iov.iov_len = batch->num_candles * sizeof(uint64_t);
    job->done = false;
    
    // Submit write
    struct io_uring_sqe *sqe = io_uring_get_sqe(uring->ring);
    io_uring_prep_writev(sqe, uring->fd, &job->iov, 1, -1);
    io_uring_sqe_set_data(sqe, job);
    io_uring_submit(uring->ring);
    
    return job;  // Non-NULL = async, will poll for completion
}

bool async_is_done(trcache *cache, trcache_candle_batch *batch, void *handle) {
    struct job *job = (struct job *)handle;
    // Poll completion queue and check if our job finished
    // ... (see benchmark/htap_benchmark.c for full example)
    return job->done;
}

void async_cleanup(void *handle, void *ctx) {
    free(handle);
}

trcache_batch_flush_ops async_ops = {
    .flush = async_flush,
    .is_done = async_is_done,
    .destroy_async_handle = async_cleanup
};
```

### Step 5: Configure Your Candle Types

```c
trcache_candle_config configs[] = {
    [0] = {  // 100-tick candle
        .user_candle_size = sizeof(MyOHLCV),
        .field_definitions = my_fields,
        .num_fields = 5,
        .update_ops = {.init = init_tick, .update = update_tick},
        .flush_ops = flush_ops,
    },
    [1] = {  // 1-minute candle
        .user_candle_size = sizeof(MyOHLCV),
        .field_definitions = my_fields,
        .num_fields = 5,
        .update_ops = {.init = init_time, .update = update_time},
        .flush_ops = flush_ops,
    },
};
```

### Step 6: Initialize the Engine

```c
trcache_init_ctx ctx = {
    .candle_configs = configs,
    .num_candle_configs = 2,
    .batch_candle_count_pow2 = 10,      // 2^10 = 1024 candles per batch
    .cached_batch_count_pow2 = 3,       // 2^3 = 8 batches cached before flush
    .total_memory_limit = 5ULL << 30,   // 5GB
    .num_worker_threads = 8,
    .max_symbols = 4096
};

trcache *cache = trcache_init(&ctx);
if (!cache) {
    // Check stderr for detailed error (e.g., memory limit too low)
}
```

**Memory Limit Calculation**: If initialization fails with "memory limit too low", the error message will show:
- Minimum required memory for your configuration
- Per-candle-type breakdown (chunk size, page size)
- Suggestions to increase limit or reduce `cached_batch_count_pow2`.

### Step 7: Register Symbols and Feed Data

```c
int aapl_id = trcache_register_symbol(cache, "AAPL");

trcache_trade_data trade = {
    .timestamp = 1609459200000,  // Unix ms
    .trade_id = 1,
    .price = {.as_double = 132.05},
    .volume = {.as_double = 100.0}
};

trcache_feed_trade_data(cache, &trade, aapl_id);
```

**Important**: Only one thread should feed data for a given symbol. Feeding from different symbols concurrently is safe.

### Step 8: Query Candle Data

#### Specify Which Fields to Retrieve

```c
int field_indices[] = {1, 2, 3};  // Request: high, low, close
trcache_field_request request = {
    .field_indices = field_indices,
    .num_fields = 3
};
```

#### Allocate Result Batch

```c
trcache_candle_batch *batch = trcache_batch_alloc_on_heap(
    cache, 
    0,          // candle_idx (0 = first config)
    100,        // capacity
    &request    // NULL = allocate all fields
);
```

#### Query by Offset (Most Recent N Candles)

```c
int ret = trcache_get_candles_by_symbol_id_and_offset(
    cache, aapl_id, 
    0,          // candle_idx
    &request, 
    0,          // offset (0 = most recent)
    10,         // count
    batch
);

if (ret == 0) {
    printf("Retrieved %d candles\n", batch->num_candles);
}
```

#### Query by Key (Specific Candle)

```c
uint64_t target_key = 1609459200000;  // Timestamp or trade_id
trcache_get_candles_by_symbol_id_and_key(
    cache, aapl_id, 0, &request, target_key, 10, batch
);
```

#### Access Column Data

**CRITICAL**: Use the **original field index** from `field_definitions`, NOT the request array index!

```c
// ❌ WRONG - using request array indices
double *highs = (double *)batch->column_arrays[0];  // This is NULL!

// ✅ CORRECT - using original field_definitions indices
double *highs  = (double *)batch->column_arrays[1];  // Index 1 in my_fields
double *lows   = (double *)batch->column_arrays[2];  // Index 2 in my_fields
double *closes = (double *)batch->column_arrays[3];  // Index 3 in my_fields

for (int i = 0; i < batch->num_candles; i++) {
    printf("Candle %d: H=%.2f L=%.2f C=%.2f\n", 
           i, highs[i], lows[i], closes[i]);
}
```

**Why?** The `column_arrays` is sized to match your **full** `field_definitions` array, but only requested fields are allocated. Non-requested fields will be NULL.

#### Cleanup

```c
trcache_batch_free(batch);
```

### Step 9: Destroy the Engine

```c
trcache_destroy(cache);  // Flushes remaining data and frees all resources
```

---

## Troubleshooting

### "Memory limit reached" during initialization

**Cause**: `total_memory_limit` is below the minimum required for your configuration.

**Solution**: The error message shows:
- Minimum required memory
- Per-candle-type breakdown
- Suggestions to adjust `cached_batch_count_pow2` or increase limit

Example error:
```
[trcache_init] Failed: total_memory_limit (100.0 MB) is less than 
the minimum required memory (512.5 MB).
Suggestion: Increase total_memory_limit or decrease cached_batch_count_pow2.
```

### Feed threads returning -1 (drops)

**Cause**: Memory limit reached at runtime.

**Solution**:
1. Check if `total_memory_limit` is too low for current workload
2. Increase limit or reduce `cached_batch_count_pow2`
3. Optimize flush callbacks to reduce latency

### Segfault on query

**Cause**: Accessing `column_arrays` with wrong index (see Step 8).

**Solution**: Always use the original `field_definitions` index, not the `field_request` index:
```c
// If field_definitions[5] is volume:
double *vol = (double *)batch->column_arrays[5];  // ✅ Correct
```

---

## Implementation Highlights

- **Lock-Free Primitives**: 
  - `atomsnap`: Custom RCU-like mechanism for atomic pointer snapshots with grace-period reclamation
  - `scalable_queue`: Per-thread MPMC queue for object pooling with O(1) operations

- **Memory Management**: 
  - Grace-period reclamation for chunks/pages
  - Pressure-aware pooling (recycle vs. free based on `memory_pressure` flag)
  - Admin thread aggregates distributed counters and updates global pressure flag

- **Adaptive Scheduling**: 
  - Admin thread calculates EMA (N=4) of cycle costs per (symbol, candle_type, stage)
  - Partitions workers into In-Memory (Apply+Convert) vs. Flush groups based on cycle demand
  - Assigns tasks via lock-free bitmaps (workers scan for set bits, CAS to claim ownership)

- **SIMD Optimization**: 
  - Column batches aligned to 64 bytes
  - Contiguous memory layout for vectorized analytics
  - Base fields (key, is_closed) always present for fast filtering

For detailed architecture documentation, see inline comments in `src/`.

---

## Benchmark & Analysis

### Running Benchmarks

```bash
# 1. Feed-only (write throughput)
./benchmark/feed_only_benchmark \
  -f 4            `# 4 feed threads` \
  -w 8            `# 8 worker threads` \
  -o feed.csv     `# output file` \
  -t 60           `# 60 sec total` \
  -W 10           `# 10 sec warmup` \
  -s 0.99         `# Zipf skew (0=uniform, 1=extreme)`

# 2. Static read (OLAP query latency)
./benchmark/static_read_benchmark \
  -r 8            `# 8 reader threads` \
  -w 8            `# 8 worker threads` \
  -o read.csv \
  -k              `# key-based access (default: offset-based)`

# 3. HTAP (mixed read/write with phases)
./benchmark/htap_benchmark \
  -f 4            `# 4 feed threads` \
  -w 8            `# 8 worker threads` \
  -o htap.csv \
  -d 100          `# 100μs delay between queries`
```

**HTAP Benchmark Phases**:
- Phase 1 (0-30s): OLTP only (baseline)
- Phase 2 (30-60s): OLTP + Light OLAP (2 readers)
- Phase 3 (60-90s): OLTP + Heavy OLAP (8 readers)
- Phase 4 (90-120s): OLTP only (recovery)

See `benchmark/` directory for source code and detailed documentation.

### In-depth Analysis 

---
