[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/minseok127/trcache)

# Trcache

Trcache is a C library for ingesting real-time trade data and converting it into **column-oriented OHLCV candle arrays** optimized for analysis. The library targets multicore machines and relies on lock-free data structures together with dedicated worker threads to scale with available CPU cores.

## Features

- **Lock-Free Pipeline**: Apply, Convert, and Flush stages run concurrently on dedicated threads without locks.
  - **Apply**: Aggregates raw trades into row-oriented candles.
  - **Convert**: Reshapes row-oriented candle pages into SIMD-friendly column batches.
  - **Flush**: Hands off completed batches to user-defined callbacks for persistence.
- **Adaptive Worker Scheduling**: An admin thread monitors pipeline throughput and dynamically schedules worker threads to balance the load.
- **Pluggable Flushing**: Applications supply synchronous or asynchronous callbacks to persist finished batches.
- **Multiple Candle Types**: Time-based and tick-based candles can be configured and processed simultaneously.

## Build

```bash
make            # build static and shared libraries
```

To generate debug binaries, set `BUILD_MODE=debug`:

```bash
make BUILD_MODE=debug
```

## Basic Usage

### 1. Candle and Field Types

`trcache` exposes enums to define candle aggregation strategies (`trcache_candle_base`) and to select specific data fields (`trcache_candle_field_type`).

```c
// From trcache.h
typedef enum {
	CANDLE_TIME_BASE,
	CANDLE_TICK_BASE,
	NUM_CANDLE_BASES
} trcache_candle_base;

typedef struct {
	trcache_candle_base base;
	int type_idx;
} trcache_candle_type;

typedef enum {
	TRCACHE_START_TIMESTAMP     = 1 << 0,
	TRCACHE_OPEN                = 1 << 1,
	TRCACHE_HIGH                = 1 << 2,
	TRCACHE_LOW                 = 1 << 3,
	TRCACHE_CLOSE               = 1 << 4,
	TRCACHE_VOLUME              = 1 << 5,
	TRCACHE_TRADING_VALUE       = 1 << 6,
	TRCACHE_TRADE_COUNT         = 1 << 7,
	TRCACHE_IS_CLOSED           = 1 << 8
} trcache_candle_field_type;
```

These constants are used to configure the engine and request specific data fields when querying candles.

### 2. Allocate Candle Batches

A `trcache_candle_batch` represents a column-oriented array of OHLCV candles. It can be allocated on the heap or the stack.

```c
/* Heap allocation */
struct trcache_candle_batch *heap_batch = trcache_batch_alloc_on_heap(512, TRCACHE_HIGH | TRCACHE_CLOSE);
// ... use batch ...
trcache_batch_free(heap_batch);

/* Stack allocation */
TRCACHE_DEFINE_BATCH_ON_STACK(stack_batch, 512, TRCACHE_HIGH | TRCACHE_CLOSE);
// ... stack_batch is valid within this scope
```

### 3. Implement Flush Operations

The `trcache_flush_ops` struct allows applications to define how completed candle batches are persisted.

A synchronous flush performs all work inside the `flush` callback and returns `NULL`.

```c
void *sync_flush(trcache *c, trcache_candle_batch *b, void *ctx) {
    // Write b->open_array, etc., to disk
    (void)c; (void)ctx;
    return NULL; // Signals synchronous completion
}
```

For asynchronous flushing, return a non-NULL handle and implement `is_done` and `destroy_handle` so the engine can poll for completion.

```c
void *async_flush(trcache *c, trcache_candle_batch *b, void *ctx);
bool async_is_done(trcache *c, trcache_candle_batch *b, void *handle);
void async_destroy(void *handle, void *ctx);
```

### 4. Initialize the Engine

Configure and initialize the `trcache` instance using `trcache_init_ctx`.

```c
// Define candle update logic using helper macros
DEFINE_TIME_CANDLE_OPS(5m, 300000); // 5-minute candle
DEFINE_TIME_CANDLE_OPS(1m, 60000); // 1-minute candle
DEFINE_TICK_CANDLE_OPS(100t, 100);   // 100-tick candle
DEFINE_TICK_CANDLE_OPS(20t, 20);   // 20-tick candle

// Define flush operations
struct trcache_flush_ops flush_ops = { .flush = sync_flush };

// Define candle configurations
trcache_candle_config time_candles[] = {
    { .threshold.interval_ms = 300000, .update_ops = ops_5m, .flush_ops = flush_ops },
    { .threshold.interval_ms = 60000, .update_ops = ops_1m, .flush_ops = flush_ops },
};
trcache_candle_config tick_candles[] = {
    { .threshold.num_ticks = 100, .update_ops = ops_100t, .flush_ops = flush_ops },
    { .threshold.num_ticks = 20, .update_ops = ops_20t, .flush_ops = flush_ops },
};

// Create initialization context
struct trcache_init_ctx ctx = {
    .num_worker_threads = 4,
    .batch_candle_count_pow2 = 10,  // 1024 candles per batch
    .cached_batch_count_pow2 = 3,   // Flush after 8 batches
    .candle_types = {
        [CANDLE_TIME_BASE] = time_candles,
        [CANDLE_TICK_BASE] = tick_candles
    },
    .num_candle_types = {
        [CANDLE_TIME_BASE] = sizeof(time_candles) / sizeof(trcache_candle_config),
        [CANDLE_TICK_BASE] = sizeof(tick_candles) / sizeof(trcache_candle_config)
    },
};

struct trcache *cache = trcache_init(&ctx);
if (!cache) {
    // Handle error
}
```

Calling `trcache_init()` spawns one admin thread and the specified number of worker threads.

### 5. Register and Query Symbols

Symbols must be registered before use.

```c
int aapl_id = trcache_register_symbol(cache, "AAPL");
const char *name = trcache_lookup_symbol_str(cache, aapl_id); // "AAPL"
int again = trcache_lookup_symbol_id(cache, "AAPL");       // same ID
```

### 6. Feed Trade Data

Push real-time trade data into the pipeline.

```c
struct trcache_trade_data td = {
    .timestamp = 1620000000000ULL, // Unix timestamp in milliseconds
    .trade_id = 1,
    .price = 123.45,
    .volume = 100,
};

trcache_feed_trade_data(cache, &td, aapl_id);
```

### 7. Destroy the Engine

Clean up all resources.

```c
trcache_destroy(cache);
```

This stops all threads, flushes any remaining data, and releases all allocated memory.

## Implementation Details

### Core Lock-Free Primitives

- **`atomsnap`**: A custom-built mechanism for atomic pointer snapshotting and grace-period memory reclamation. It's used to manage shared data structures that undergo structural changes, such as the `symbol_table`'s main array or the `candle_chunk_list`'s head pointer, allowing readers to access data without locks while writers perform updates.
- **`scalable_queue`**: A highly concurrent queue designed to minimize contention between threads. It is used for dispatching scheduling commands from the admin thread to the workers.

### Data Flow and Pipeline

The journey of a single trade begins when `trcache_feed_trade_data` is called.

1.  **Ingestion**: The trade is copied into a thread-local `trade_data_buffer` associated with its symbol. This buffer is a linked list of chunks, allowing for lock-free writes from the user thread.
2.  **Apply Stage**: A worker thread assigned to the `APPLY` stage consumes trades from the `trade_data_buffer`. It updates the currently active (mutable) `trcache_candle` within a `candle_chunk`. Only the most recent candle is mutable; all prior candles are considered immutable.
3.  **Convert Stage**: Once a candle is complete, a worker thread in the `CONVERT` stage transforms the immutable row-oriented candle data into a column-oriented `trcache_candle_batch`. This AoS-to-SoA (Array of Structs to Struct of Arrays) transformation is key for analytical performance.
4.  **Flush Stage**: When a `candle_chunk` is fully converted into a columnar batch and the number of unflushed batches exceeds a threshold, a `FLUSH` worker invokes the user-provided `trcache_flush_ops` callbacks to persist the data.

> Trading strategies are validated through backtesting, which simulates trades using historical, completed candles. Live trading aims to replicate this backtested logic, meaning trading decisions are made precisely at the moment a candle completes. To ensure the accuracy of these decisions, it is crucial that all buffered trades are applied to a candle just before it finalizes. `trcache` achieves this by allowing the user thread that feeds the data to directly apply buffered trades when a candle (either time-based or tick-based) is nearing completion. At all other times, the apply workload is deferred to worker threads, maximizing throughput without sacrificing decision-making accuracy.

### Memory Model: From Rows to Columns

- **`candle_chunk`**: This is the central data structure, acting as a staging area. It contains multiple `candle_row_page`s, which are 4KB pages holding row-oriented `trcache_candle` structs. This row-major layout is efficient for write-heavy updates in the `APPLY` stage.
- **`atomsnap` for Row Pages**: The pointers to these row pages within a chunk are managed by `atomsnap`. This allows the `CONVERT` worker to read a stable version of a page for conversion while the `APPLY` worker might be writing to a newer page, all without locks. Once a page is fully converted, `atomsnap` ensures it's safely reclaimed after all reader threads have finished with it.
- **`trcache_candle_batch`**: The final output is a struct where each candle field (`open`, `high`, `low`, `close`, etc.) is a separate array. All these arrays are allocated in a single contiguous memory block and are aligned to 64 bytes to enable efficient SIMD vector instructions for analytical queries.

### Concurrency and Scheduling

- **Admin Thread**: This thread acts as the central scheduler. It periodically:
    - Calculates the throughput (rate) of each pipeline stage for every symbol using Exponential Moving Averages (EMAs).
    - Estimates the demand for each stage based on these rates.
    - Computes the optimal number of worker threads to allocate to each stage (`APPLY`, `CONVERT`, `FLUSH`) to prevent bottlenecks.
    - Dispatches `ADD_WORK` and `REMOVE_WORK` messages to worker threads to dynamically rebalance assignments.
- **Worker Threads**: Workers are the execution units. Each worker maintains a list of assigned work items (a combination of symbol, candle type, and pipeline stage). They continuously iterate through their work list, executing the corresponding tasks (`worker_do_apply`, `worker_do_convert`, etc.).
- **Communication**: The admin and worker threads communicate via lock-free queues (`scalable_queue`). This ensures that scheduling messages can be sent and received with minimal contention.

## Evaluation
