[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/minseok127/trcache)

# TRCACHE

`trcache` is a C library for ingesting real-time trade data and converting it into **column-oriented OHLCV candle arrays** optimized for analysis. The library targets multicore machines and relies on lock-free data structures together with dedicated worker threads to scale with available CPU cores.

# Features

- **Lock-Free Pipeline**: Apply, Convert, and Flush stages run concurrently on dedicated threads without locks.
  - **Apply**: Aggregates raw trades into row-oriented candles.
  - **Convert**: Reshapes row-oriented candle pages into SIMD-friendly column batches.
  - **Flush**: Hands off completed batches to user-defined callbacks for persistence.
- **Adaptive Worker Scheduling**: An admin thread monitors pipeline throughput and dynamically schedules worker threads to balance the load.
- **Pluggable Flushing**: Applications supply synchronous or asynchronous callbacks to persist finished batches.
- **Multiple Candle Types**: Time-based and tick-based candles can be configured and processed simultaneously.

# Build

```bash
make            # build static and shared libraries
```

To generate debug binaries, set `BUILD_MODE=debug`:

```bash
make BUILD_MODE=debug
```

# Basic Usage

### 1. Candle and Field Types

`trcache` exposes enums to define candle aggregation strategies (`trcache_candle_type`) and to select specific data fields (`trcache_candle_field_type`).

A `trcache_candle_type` struct is used to uniquely identify a specific kind of candle you want to work with. It has two parts: a `base` and a `type_idx`.

```C
// From trcache.h
typedef enum {
	CANDLE_TIME_BASE, // For candles based on time intervals (e.g., 1-minute, 5-minute)
	CANDLE_TICK_BASE, // For candles based on a number of trades (e.g., 100-tick)
	NUM_CANDLE_BASES
} trcache_candle_base;

typedef struct trcache_candle_type {
	trcache_candle_base base;
	int type_idx;
} trcache_candle_type;
```

- `base`: This field determines the fundamental aggregation strategy. For example, `CANDLE_TIME_BASE` groups all candles that are formed based on fixed time intervals.
- `type_idx`: This field is a zero-based index that specifies which configured candle of that base you are referring to. When you initialize `trcache`, you provide an array of configurations for each base (e.g., an array for time-based candles, another for tick-based). The `type_idx` corresponds to the index in that configuration array.

For instance, if you configure two time-based candles—a 1-minute candle at index 0 and a 5-minute candle at index 1—you would use `{ .base = CANDLE_TIME_BASE, .type_idx = 0 }` to refer to the 1-minute candles and `{ .base = CANDLE_TIME_BASE, .type_idx = 1 }` for the 5-minute candles.

Additionally, you can select which data fields you want to retrieve using the `trcache_candle_field_type` enum flags:

```C
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

These constants are used to configure the engine and request specific data fields when querying candles. To request multiple field types, you can combine them using the bitwise OR operator (e.g., `TRCACHE_HIGH | TRCACHE_LOW`).

### 2. Allocate Candle Batches

A `trcache_candle_batch` represents a column-oriented array of OHLCV candles. It can be allocated on the heap or the stack.

```C
/* Heap allocation */
struct trcache_candle_batch *heap_batch = trcache_batch_alloc_on_heap(512, TRCACHE_HIGH | TRCACHE_CLOSE);
// ... use batch ...
trcache_batch_free(heap_batch);

/* Stack allocation */
TRCACHE_DEFINE_BATCH_ON_STACK(stack_batch, 512, TRCACHE_HIGH | TRCACHE_CLOSE);
// ... stack_batch is valid within this scope
```

When a batch is created, the arrays for the requested fields (e.g., open_array, high_array) are allocated together as a single, contiguous block of memory. The starting address of each array is also aligned to 64 bytes. This memory layout is intentionally designed to be friendly for SIMD operations.

### 3. Implement Update and Flush Operations

`trcache` provides a callback-based interface for users to define how candle data is processed and stored. This is done through the `trcache_candle_update_ops` and `trcache_batch_flush_ops` structures. Users should provide these callbacks for each candle type.

#### `trcache_candle_update_ops`

This structure defines the logic for how a candle is initialized from the first trade and updated with subsequent trades.
- `init(struct trcache_candle *c, struct trcache_trade_data *d)`: This function is called once per candle to initialize it using the first trade data `d`.
- `update(struct trcache_candle *c, struct trcache_trade_data *d)`: This function is called for subsequent trades to update an existing candle `c`. It must return `true` if the trade was consumed by the current candle, or `false` if the candle is considered complete and the trade should start a new candle.

```C
typedef struct trcache_candle_update_ops {
	void (*init)(struct trcache_candle *c, struct trcache_trade_data *d);
	bool (*update)(struct trcache_candle *c, struct trcache_trade_data *d);
} trcache_candle_update_ops;
```

The library provides helper macros to easily define standard time-based and tick-based candle logic.
```C
// Define logic for a 5-minute (300000 milliseconds) candle
DEFINE_TIME_CANDLE_OPS(5m, 300000);

// Define logic for a 100-tick candle
DEFINE_TICK_CANDLE_OPS(100t, 100);

// Assign the defined logic to a struct instance
const struct trcache_candle_update_ops ops_5m_candle = {
    .init = init_5m,
    .update = update_5m,
};
```

#### `trcache_batch_flush_ops`

This structure defines how completed candle batches are persisted (e.g., written to a file, sent to a database). It supports both synchronous and asynchronous operations.

```C
typedef struct trcache_batch_flush_ops {
	void *(*flush)(trcache *cache, trcache_candle_batch *batch, void *flush_ctx);
	bool (*is_done)(trcache *cache, trcache_candle_batch *batch, void *handle);
	void (*destroy_handle)(void *handle, void *destroy_handle_ctx);
	void *flush_ctx;
	void *destroy_handle_ctx;
} trcache_batch_flush_ops;
```

- Synchronous Flush: The flush callback performs all I/O operations and returns `NULL`. The engine considers the batch flushed immediately.
```C
void* sync_flush(trcache *c, trcache_candle_batch *b, void *flush_ctx) {
    // Write candle data from batch 'b' to disk.
    // 'flush_ctx' is a user-defined pointer set in trcache_init_ctx.
    (void)c;
    return NULL; // Signals synchronous completion
}

// Assign the defined logic to a struct instance
const struct trcache_batch_flush_ops ops_5m_candle = {
    .flush = sync_flush,
	.is_done = NULL,
	.destroy_handle = NULL,
    .flush_ctx = flush_ctx,
	.destroy_handle_ctx = NULL
};
```

- Asynchronous Flush: The flush callback initiates an I/O operation and returns a `non-NULL` handle (e.g., a pointer to a job tracking object). The engine then polls `is_done` until it returns `true`, after which it calls `destroy_handle` for cleanup.
	- `void* flush(...)`: Initiates the async I/O and returns a unique `handle` to track the operation.
	- `bool is_done(..., void *handle)`: Receives the `handle` returned by `flush` and checks if the operation is complete. Returns `true` when done.
	- `void destroy_handle(void *handle, ...)`: Called after `is_done` returns `true`. Frees any resources associated with the `handle`.
```C
// Example struct for tracking an async job
typedef struct { int fd; /* ... */ } AsyncJob;

void* async_flush(trcache *c, trcache_candle_batch *b, void *flush_ctx) {
    AsyncJob *job = malloc(sizeof(AsyncJob));
    // Start an async write operation using 'job'...
    return job; // Return the job object as the handle
}

bool async_is_done(trcache *c, trcache_candle_batch *b, void *handle) {
    AsyncJob *job = (AsyncJob*)handle;
    // Check the completion status of the job...
    return is_job_complete(job);
}

void async_destroy(void *handle, void *destroy_handle_ctx) {
    free(handle); // Free the job object
}

// Assign the defined logic to a struct instance
const struct trcache_batch_flush_ops ops_5m_candle = {
    .flush = async_flush,
	.is_done = async_is_done,
	.destroy_handle = async_destroy,
    .flush_ctx = flush_ctx,
	.destroy_handle_ctx = destroy_handle_ctx
};
```

### 4. Initialize the Engine

Configure and initialize the `trcache` instance using `trcache_init_ctx`.

```C
// Define candle update logic using helper macros
DEFINE_TIME_CANDLE_OPS(5m, 300000);   // 5-minute candle
DEFINE_TIME_CANDLE_OPS(1m, 60000);    // 1-minute candle
DEFINE_TICK_CANDLE_OPS(100t, 100);    // 100-tick candle
DEFINE_TICK_CANDLE_OPS(20t, 20);      // 20-tick candle

// Define flush operations
struct trcache_batch_flush_ops flush_ops = { .flush = sync_flush };

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

```C
int aapl_id = trcache_register_symbol(cache, "AAPL");
const char *name = trcache_lookup_symbol_str(cache, aapl_id); // "AAPL"
int again = trcache_lookup_symbol_id(cache, "AAPL");       // same ID
```

### 6. Feed Trade Data

Push real-time trade data into the pipeline.

```C
struct trcache_trade_data td = {
    .timestamp = 1620000000000ULL, // Unix timestamp in milliseconds
    .trade_id = 1,
    .price = 123.45,
    .volume = 100,
};

trcache_feed_trade_data(cache, &td, aapl_id);
```

### 7. Querying Candle Data

`trcache` provides functions to retrieve candle data for a given symbol. You can query data based on a specific end-point in time or relative to the most recent candle. All query functions are thread-safe.

- `trcache_get_candles_by_symbol_..._and_ts(...,uint64_t ts_end, int count,...)`: Retrieves a `count` of candles ending at the candle whose time range includes the specified `ts_end` (a Unix timestamp in milliseconds).
- `trcache_get_candles_by_symbol_..._and_offset(...,int offset, int count,...)`: Retrieves a `count` of candles ending at a specific `offset` from the most recent candle, where `offset = 0` refers to the latest (potentially still open) candle.

Both query types can identify the symbol by its string name (_str) or its integer ID.

```C
// Define a candle type to query (e.g., 5-minute time-based candles)
trcache_candle_type candle_type = { .base = CANDLE_TIME_BASE, .type_idx = 0 };

// Allocate a destination batch on the stack
TRCACHE_DEFINE_BATCH_ON_STACK(batch, 100, TRCACHE_FIELD_MASK_ALL);

// Example: Get the last 50 candles ending at a specific timestamp
uint64_t end_ts = 1620005400000ULL; // An exact timestamp
int count = 50;

if (trcache_get_candles_by_symbol_str_and_ts(cache, "AAPL", candle_type,
        TRCACHE_START_TIMESTAMP | TRCACHE_CLOSE, end_ts, count, &batch) == 0) {
    printf("Successfully retrieved %d candles for AAPL\n", batch.num_candles);
    for (int i = 0; i < batch.num_candles; i++) {
        // Access data via columnar arrays
        printf("Timestamp: %llu, Close: %f\n",
               batch.start_timestamp_array[i], batch.close_array[i]);
    }
}

// Example: Get the 10 most recent candles
if (trcache_get_candles_by_symbol_id_and_offset(cache, aapl_id, candle_type,
        TRCACHE_FIELD_MASK_ALL , 0, 10, &batch) == 0) {
    printf("Successfully retrieved the %d most recent candles for AAPL\n", batch.num_candles);
    // ... process data
}
```

### 8. Destroy the Engine

Clean up all resources.

```C
trcache_destroy(cache);
```

This stops all threads, flushes any remaining data, and releases all allocated memory.

# Implementation Details

### Core Lock-Free Primitives

- [**`atomsnap`**](https://github.com/minseok127/atomsnap): A custom-built mechanism for atomic pointer snapshotting and grace-period memory reclamation. It's used to manage shared data structures that undergo structural changes, such as the `symbol_table`'s main array or the `candle_chunk_list`'s head pointer, allowing readers to access data without locks while writers perform updates.
- [**`scalable_queue`**](https://github.com/minseok127/scalable-queue): A highly concurrent queue designed to minimize contention between threads. Its default version is used for dispatching scheduling commands from the admin thread to the workers. A linearizable version is employed within the `candle_chunk_list` to enable lock-free memory reclamation of flushed `candle_chunk`s.

### Data Flow and Pipeline

The journey of a single trade begins when `trcache_feed_trade_data` is called.

1.  **Ingestion**: The trade is copied into a thread-local `trade_data_buffer` associated with its symbol. This buffer is a linked list of `trade_data_chunk`s, allowing for lock-free writes from the user thread.
2.  **Apply Stage**: A worker thread assigned to the `APPLY` stage consumes trades from the `trade_data_buffer`. It updates the currently active (mutable) `trcache_candle` within a `candle_chunk`. Only the most recent candle is mutable; all prior candles are considered immutable.
3.  **Convert Stage**: Once a candle is complete, a worker thread in the `CONVERT` stage transforms the immutable row-oriented candle data into a column-oriented `trcache_candle_batch` (Array of Structs to Struct of Arrays).
4.  **Flush Stage**: When a `candle_chunk` is fully converted into a columnar batch and the number of unflushed batches exceeds a threshold, a `FLUSH` worker invokes the user-provided `trcache_batch_flush_ops` callbacks to persist the data.

> Trading strategies are validated through backtesting, which simulates trades using historical, completed candles. Live trading aims to replicate this backtested logic, meaning trading decisions are made precisely at the moment a candle completes. To ensure the accuracy of these decisions, it is crucial that all buffered trades are applied to a candle just before it finalizes. `trcache` achieves this by allowing the user thread that feeds the data to directly apply buffered trades when a candle (either time-based or tick-based) is nearing completion. At all other times, the apply workload is deferred to worker threads, maximizing throughput without sacrificing decision-making accuracy.

### Memory Model: From Rows to Columns

- **`candle_chunk`**: This is the central data structure, acting as a staging area. It contains multiple `candle_row_page`s, which are 4KB pages holding row-oriented `trcache_candle` structs. This row-major layout is efficient for write-heavy updates in the `APPLY` stage.
- **`atomsnap` for Row Pages**: The pointers to these row pages within a chunk are managed by `atomsnap`. This allows the `CONVERT` worker to read a stable version of a page for conversion while the `APPLY` worker might be writing to a newer page, all without locks. Once a page is fully converted, `atomsnap` ensures it's safely reclaimed after all reader threads have finished with it.
- **`trcache_candle_batch`**: The final output is a struct where each candle field (`open`, `high`, `low`, `close`, etc.) is a separate array. All these arrays are allocated in a single contiguous memory block and are aligned to 64 bytes to enable efficient SIMD vector instructions for analytical queries.

### Memory Reclamation of Flushed Chunks

`trcache` employs a lock-free, grace-period-based memory reclamation scheme to safely deallocate `candle_chunk` structures after they have been flushed. This mechanism is built upon `atomsnap`, an RCU-like (Read-Copy-Update) primitive that ensures reader threads can safely traverse the list of chunks even while `FLUSH` workers are removing old chunks.

1. **Versioned List Head**: The `candle_chunk_list` maintains a linked list of chunks. The head of this list, representing the oldest available data, is not a direct pointer but is managed through an `atomsnap` version (`candle_chunk_list_head_version`). Reader threads that query candle data first acquire this version to get a stable snapshot of the list's head.

2. **Graceful Retirement**: When a `FLUSH` worker successfully persists a range of chunks, it doesn't immediately deallocate them. Instead, it advances the logical head of the list past these flushed chunks by publishing a new `atomsnap` version pointing to the next live chunk. The old version, which references the now-obsolete chunks, is retired.

3. **Delayed Deallocation**: The `atomsnap` primitive guarantees a grace period. The retired version is only deallocated after all threads that might have acquired it have finished their operations and released their references.

4. **Callback-Driven Freeing**: The actual memory deallocation is performed inside `candle_chunk_list_head_free`, a callback function that is invoked by `atomsnap` when a retired version's reference count drops to zero. This callback iterates through the list of flushed chunks covered by that version and safely calls `candle_chunk_destroy` on each one, releasing its memory.

### Concurrency and Scheduling

- **Admin Thread**: This thread acts as the central scheduler. It periodically:
    - Calculates the throughput (rate) of each pipeline stage for every symbol using Exponential Moving Averages (EMAs).
    - Estimates the demand for each stage based on these rates.
    - Computes the optimal number of worker threads to allocate to each stage (`APPLY`, `CONVERT`, `FLUSH`) to prevent bottlenecks.
    - Dispatches `ADD_WORK` and `REMOVE_WORK` messages to worker threads to dynamically rebalance assignments.
- **Worker Threads**: Workers are the execution units. Each worker maintains a list of assigned work items (a combination of symbol, candle type, and pipeline stage). They continuously iterate through their work list, executing the corresponding tasks (`worker_do_apply`, `worker_do_convert`, etc.).
- **Communication**: The admin and worker threads communicate via lock-free queues (`scalable_queue`). This ensures that scheduling messages can be sent and received with minimal contention.

# Evaluation
