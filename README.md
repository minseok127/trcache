[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/minseok127/trcache)

# TRCACHE

`trcache` is a C library for ingesting real-time trade data and converting it into **column-oriented user-defined candle arrays** optimized for analysis. The library targets multicore machines and relies on lock-free data structures together with dedicated worker threads to scale with available CPU cores.

# Features

- **Lock-Free Pipeline**: Apply, Convert, and Flush stages run concurrently on dedicated threads without locks.
  - **Apply**: Aggregates raw trades into row-oriented candles.
  - **Convert**: Reshapes row-oriented candle pages into SIMD-friendly column batches.
  - **Flush**: Hands off completed batches to user-defined callbacks for persistence.
- **Fully Customizable Candles**: Define any candle structure you need. `trcache` is not limited to specific candle type and can handle any number and type of fields.
- **Lock-Free Queries**: Read candle data without locks, even as it's being transformed by the concurrent pipeline.
- **Adaptive Worker Scheduling**: An admin thread monitors pipeline throughput and dynamically schedules worker threads to balance the load.

# Build

```bash
make            # build static and shared libraries
```

To generate debug binaries, set `BUILD_MODE=debug`:

```bash
make BUILD_MODE=debug
```

# Core Concepts and Usage

`trcache` gives you full control over your candle data structure. The following steps guide you through defining, configuring, and using a custom candle type.

### Step 1: Define Your Custom Candle Structure

First, define the C struct for your candle. The only requirement is that its very first member must be of type `trcache_candle_base`. This base structure allows `trcache` to access the candle's unique key and its closed status in a type-agnostic way.

```c
// From trcache.h
typedef struct trcache_candle_base {
    union {
        uint64_t timestamp;
        uint64_t trade_id;
        uint64_t value;
    } key;
    bool is_closed;
} trcache_candle_base;

// Example: A custom OHLCV candle with trade count
typedef struct {
    trcache_candle_base base; // MUST be the first member
    double open;
    double high;
    double low;
    double close;
    uint64_t volume;
    uint32_t trade_count;
} MyOHLCV;
```

### Step 2: Describe the Candle's Memory Layout

Next, you must describe the memory layout of your custom fields to `trcache`. This is done by creating an array of `trcache_field_def` structs. This schema allows the library to perform high-performance transformations from your row-oriented struct to a column-oriented batch.

Use the `offsetof()` and `sizeof()` macros to ensure correctness.

```c
// From trcache.h
typedef struct trcache_field_def {
    size_t offset;
    size_t size;
    trcache_field_type type;
} trcache_field_def;

// An array describing the fields in the 'MyOHLCV' struct
const struct trcache_field_def my_ohlcv_fields[] = {
    {offsetof(MyOHLCV, open), sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, high), sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, low), sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, close), sizeof(double), FIELD_TYPE_DOUBLE},
    {offsetof(MyOHLCV, volume), sizeof(uint64_t), FIELD_TYPE_UINT64},
    {offsetof(MyOHLCV, trade_count), sizeof(uint32_t), FIELD_TYPE_UINT32},
};
```

- **Note on `trcache_field_type`**: This enum provides metadata about your fields. While it is not strictly required for the core operation of `trcache`, it is recommended for debugging purposes and for internal analytics.

### Step 3: Understanding the Input - `trcache_trade_data`

The fundamental unit of data you'll provide to `trcache` is a single trade, represented by the `trcache_trade_data` struct. This is the raw information from which all your custom candles will be built.

```c
// From trcache.h
typedef union {
	double as_double;
	int64_t as_int64;
	void *as_ptr;
} trcache_value;

typedef struct trcache_trade_data {
	uint64_t timestamp;
	uint64_t trade_id;
	trcache_value price;
	trcache_value volume;
} trcache_trade_data;
```

When you feed data into the engine using `trcache_feed_trade_data()`, you will populate this struct for each trade. Your candle update logic in the next step will then consume this data to build your candles.

### Step 4: Understanding the Output - `trcache_candle_batch`

The ultimate goal of trcache is to produce `trcache_candle_batch` structures. This is a column-oriented representation of your candle data. Understanding this structure is key to both querying data and implementing your `flush` logic.

```c
// From trcache.h
typedef struct trcache_candle_batch {
	uint64_t *key_array;
	bool *is_closed_array;
	void **column_arrays;
	int capacity;
	int num_candles;
	int candle_idx;
	int symbol_id;
} trcache_candle_batch;
```

- **Columnar Layout**: Instead of an array of your `MyOHLCV` structs (Array of Structs), a batch holds a struct of arrays (Struct of Arrays). For example, all open prices are stored contiguously in one array, all high prices in another, and so on. This layout is efficient for analytical queries and vectorized computations (SIMD).
- `key_array` & `is_closed_array`: Pointers to arrays for the base candle fields, which are always present.
- `column_arrays`: This is an array of `void*` pointers. Each element `column_arrays[i]` points to the columnar data array for the *i-th* field you defined in your `my_ohlcv_fields` array. You need to cast this pointer to the correct type (e.g., `(double *)batch->column_arrays[0]` for the `open` prices).
- **Memory Management**: All arrays in the batch point to a single, contiguous, SIMD-aligned memory block. You can allocate a batch on the heap using `trcache_batch_alloc_on_heap()` and free it with `trcache_batch_free()`.

### Step 5: Implement the Candle Update Logic

Define how your candle is initialized from the first trade and updated by subsequent trades. This logic is provided via a `trcache_candle_update_ops` struct.

```c
// From trcache.h
typedef struct trcache_candle_update_ops {
	void (*init)(struct trcache_candle_base *c, struct trcache_trade_data *d);
	bool (*update)(struct trcache_candle_base *c, struct trcache_trade_data *d);
} trcache_candle_update_ops;
```

- `void init(trcache_candle_base *c, trcache_trade_data *d)`
	- This function is called once to initialize a new candle.
	- `c`: A pointer to the uninitialized memory for your new candle. You should cast it to your custom candle type (e.g., `MyOHLCV *candle = (MyOHLCV *)c;`).
	- `d`: A pointer to the first trade data that triggered the creation of this candle.
	- **You must set the c->key.** The key must be unique and monotonically increasing for all subsequent candles of this same type.
- `bool update(trcache_candle_base *c, trcache_trade_data *d)`
	- This function is called for each subsequent trade to update an existing, active candle.
	- `c`: A pointer to the current, active candle.
	- `d`: A pointer to the incoming `trcache_trade_data`.
	- Return Value:
		- Return `true` if the trade was successfully applied to the current candle `c`.
		- Return `false` if the trade does not belong to the current candle (i.e., the candle's closing condition has been met). Before returning `false`, you must set `c->is_closed = true;`. `trcache` will then automatically call your `init` function to start a new candle using this same trade data `d`.

```c
// Example logic for a 1-minute time-based candle
void my_1min_init(trcache_candle_base *c, struct trcache_trade_data *d) {
    MyOHLCV *candle = (MyOHLCV *)c;
    // Set the key to the start of the 1-minute interval
    c->key.timestamp = d->timestamp - (d->timestamp % 60000);
    c->is_closed = false;
    
    candle->open = d->price.as_double;
    candle->high = d->price.as_double;
    candle->low = d->price.as_double;
    candle->close = d->price.as_double;
    candle->volume = d->volume.as_double;
    candle->trade_count = 1;
}

bool my_1min_update(trcache_candle_base *c, struct trcache_trade_data *d) {
    // Check if the trade is outside the current candle's time window
    if (d->timestamp >= c->key.timestamp + 60000) {
        c->is_closed = true; // Mark the candle as complete
        return false;        // Signal that this trade should start a new candle
    }

    MyOHLCV *candle = (MyOHLCV *)c;
    double price = d->price.as_double;
    
    if (price > candle->high) candle->high = price;
    if (price < candle->low) candle->low = price;
    candle->close = price;
    candle->volume += d->volume.as_double;
    candle->trade_count++;
    
    return true; // Trade was successfully consumed by this candle
}

const struct trcache_candle_update_ops my_1min_update_ops = { .init = my_1min_init, .update = my_1min_update };
```

### Step 6: Implement the Batch Persistence Logic

Define how completed batches of your candles are saved. These operations are triggered by a worker thread when the number of completed, in-memory batches exceeds the `cached_batch_count_pow2` threshold. At this point, the oldest batch is passed to your `flush` callback and evicted from the cache.

```c
// From trcache.h
typedef struct trcache_batch_flush_ops {
	void *(*flush)(trcache *cache, trcache_candle_batch *batch, void *flush_ctx);
	bool (*is_done)(trcache *cache, trcache_candle_batch *batch, void *async_handle);
	void (*destroy_async_handle)(void *async_handle, void *destroy_async_handle_ctx);
	void (*on_batch_destroy)(trcache_candle_batch *batch, void *on_destroy_ctx);
	void *flush_ctx;
	void *destroy_async_handle_ctx;
	void *on_destroy_ctx;
} trcache_batch_flush_ops;
```

- `void* flush(trcache *cache, trcache_candle_batch *batch, void *flush_ctx)`
	- Called to initiate the persistence of a full batch.
	- `batch`: A pointer to the column-oriented `trcache_candle_batch` ready to be saved.
	- `flush_ctx`: Your user-defined context pointer, passed from the `trcache_batch_flush_ops`.
	- Return Value:
		- `NULL`: For a synchronous flush. The engine assumes the I/O is complete when the function returns.
		- `non-NULL` handle: For an asynchronous flush. Return a unique pointer or token (e.g., a job context struct) that identifies the I/O operation.
- `bool is_done(trcache *cache, trcache_candle_batch *batch, void *async_handle)`
	- (Asynchronous only) Polled periodically by the engine if flush returned a `non-NULL` handle.
	- `async_handle`: The handle you returned from your `flush` function.
	- Return Value: Return `true` when the I/O operation associated with the handle is complete, `false` otherwise.
- `void destroy_async_handle(void *async_handle, void *destroy_async_handle_ctx)`
	- (Asynchronous only) Called once after `is_done` returns `true`.
	- `async_handle`: The handle you returned from `flush`. Use this callback to clean up any resources associated with the asynchronous operation (e.g., free(async_handle)).
	- `destroy_async_handle_ctx`: Your user-defined context pointer, passed from the `trcache_batch_flush_ops`.
- `void on_batch_destroy(trcache_candle_batch *batch, void *on_destroy_ctx)`
	- Called just before the memory for a `trcache_candle_batch` is freed by the engine. This is your last chance to clean up any resources held within the candles themselves (e.g., if you used `FIELD_TYPE_POINTER` for a field and need to free the pointed-to memory).
	- `on_destroy_ctx`: Your user-defined context pointer, passed from the `trcache_batch_flush_ops`.

Unlike synchronous flushing, which processes one batch at a time (blocking until the I/O is finished before starting the next), the asynchronous model allows the system to achieve higher throughput. The `FLUSH` worker can call your `flush` callback for multiple batches concurrently, submitting many I/O requests to the operating system without waiting. It then periodically polls the `is_done` callback for all pending batches to check their completion status.

#### Synchronous Flush Example

```c
void* my_sync_flush(trcache *cache, trcache_candle_batch *batch, void *ctx) {
    printf("Sync flushing %d candles for candle_idx=%d\n", batch->num_candles, batch->candle_idx);
    // write_batch_to_disk(batch);
    return NULL; // Synchronous completion
}

const struct trcache_batch_flush_ops my_sync_flush_ops = {
    .flush = my_sync_flush
};
```

#### Asynchronous Flush Example

This example demonstrates a simplified asynchronous file write using `liburing`.

```c
// Requires linking with -luring
#include <liburing.h>
#include <fcntl.h>

// Context for the flush operation and a single I/O request
typedef struct {
    struct io_uring *ring;
    int fd;
} UringFlushCtx;

typedef struct {
    UringFlushCtx *parent_ctx;
    struct iovec iov;
    bool is_complete;
} UringJob;

// flush: Submits a write request and returns a handle
void* uring_async_flush(trcache *cache, trcache_candle_batch *batch, void *flush_ctx) {
    UringFlushCtx *ctx = (UringFlushCtx *)flush_ctx;
    
    UringJob *job = malloc(sizeof(UringJob));
    job->parent_ctx = ctx;
    job->is_complete = false;
    
    // Prepare the data to be written (in a real scenario, you'd serialize the batch)
    job->iov.iov_base = batch->key_array;
    job->iov.iov_len = batch->num_candles * sizeof(uint64_t);
    
    struct io_uring_sqe *sqe = io_uring_get_sqe(ctx->ring);
    io_uring_prep_writev(sqe, ctx->fd, &job->iov, 1, -1);
    io_uring_sqe_set_data(sqe, job); // Associate job with the request
    
    io_uring_submit(ctx->ring);
    
    return job; // Return the job as the handle
}

// is_done: Checks for completion events from the ring
bool uring_async_is_done(trcache *cache, trcache_candle_batch *batch, void *handle) {
    UringJob *job = (UringJob *)handle;
    struct io_uring_cqe *cqe;

    // Peek for a completion event for this specific job
    int ret = io_uring_peek_cqe(job->parent_ctx->ring, &cqe);
    if (ret == 0 && cqe != NULL) {
        UringJob *completed_job = (UringJob *)io_uring_cqe_get_data(cqe);
        if (completed_job == job) {
            // Our job is complete
            if (cqe->res < 0) {
                fprintf(stderr, "Async write failed: %s\n", strerror(-cqe->res));
            }
            job->is_complete = true;
            io_uring_cqe_seen(job->parent_ctx->ring, cqe);
        }
    }
    return job->is_complete;
}

// destroy_async_handle: Cleans up the job context
void uring_async_destroy_handle(void *handle, void *ctx) {
    free(handle);
}

// In your main application, you would initialize the UringFlushCtx
// UringFlushCtx flush_context;
// io_uring_queue_init(QUEUE_DEPTH, &flush_context.ring, 0);
// flush_context.fd = open("candle_data.bin", O_WRONLY | O_CREAT, 0644);

const struct trcache_batch_flush_ops my_async_flush_ops = {
    .flush = uring_async_flush,
    .is_done = uring_async_is_done,
    .destroy_async_handle = uring_async_destroy_handle,
    .flush_ctx = &flush_context, // Pass the io_uring context
};
```

### Step 7: Configure Your Candle Type

Combine the metadata and logic into a `trcache_candle_config` struct. The index in this array becomes the unique ID for your candle type.

```c
const trcache_candle_config my_candle_configs[] = {
    [0] = { // This candle will have ID = 0 (index)
        .user_candle_size = sizeof(MyOHLCV),
        .field_definitions = my_ohlcv_fields,
        .num_fields = sizeof(my_ohlcv_fields) / sizeof(trcache_field_def),
        .update_ops = my_1min_update_ops,
        .flush_ops = my_sync_flush_ops, // Choose sync or async ops
    },
	[1] = { ... },
	[2] = { ... },
	...
};
```

### Step 8: Initialize the Engine

Pass your configuration array and other settings to `trcache_init()` via the `trcache_init_ctx` struct.

```c
// From trcache.h
typedef struct trcache_init_ctx {
	const struct trcache_candle_config *candle_configs;
	int num_candle_configs;
	int batch_candle_count_pow2;
	int cached_batch_count_pow2;
	size_t aux_memory_limit;
	int num_worker_threads;
} trcache_init_ctx;
```

- `candle_configs`: A pointer to your array of `trcache_candle_config` structs.
- `num_candle_configs`: The total number of configurations in your array.
- `batch_candle_count_pow2`: The number of candles per columnar batch, expressed as a power of two (e.g., 10 for 1024).
- `cached_batch_count_pow2`: The number of full batches to keep in memory before triggering a flush on the oldest one, as a power of two (e.g., 3 for 8 batches).
- `aux_memory_limit`: A memory limit in bytes for auxiliary data structures (e.g., scheduler messages). 0 means no limit.
- `num_worker_threads`: The number of worker threads for the pipeline.

```c
struct trcache_init_ctx ctx = { /* ... */ };
struct trcache *cache = trcache_init(&ctx);
```

Upon a successful return from `trcache_init()`, the library is fully active. One admin thread and a pool of worker threads (the number specified by `num_worker_threads`) are created and running in the background.

The admin thread continuously monitors the system's workload. Based on the rate of incoming data and the processing speed of each pipeline stage, it dynamically assigns tasks to the worker threads to ensure the pipeline (Apply, Convert, Flush) remains balanced and operates at maximum efficiency.

### Step 9: Register Symbols and Feed Data

Register each symbol and feed trade data. The `trcache_trade_data` struct you pass is copied into an internal buffer, so you can safely allocate it on the stack and let it go out of scope after the call.

```c
// Register specific symbol
int aapl_id = trcache_register_symbol(cache, "AAPL");

// Receive trade data from the trading server
while (receive_trade_data_from_your_api()) {
	// Define trade data
	struct trcache_trade_data trade = { /* ... */ };
	trcache_feed_trade_data(cache, &trade, aapl_id);
}
```

**Concurrency Limitation**: While `trcache` is thread-safe for most operations, there is one important restriction: **you should not have multiple threads calling `trcache_feed_trade_data` for the same symbol ID at the same time**. Feeding data for different symbols concurrently from different threads is perfectly safe and encouraged.

### Step 10: Query Candle Data

To retrieve data, you must specify which fields you want using a `trcache_field_request`.

```c
// From trcache.h
typedef struct trcache_field_request {
	const int *field_indices;
	int num_fields;
} trcache_field_request;
```

- `field_indices`: An array of integers. Each integer is an index into the `field_definitions` array that you provided in Step 2. This tells trcache which specific custom fields you want to retrieve.
- `num_fields`: The number of indices in your `field_indices` array.

For example, to query the `high`, `low`, and `close` fields from our `MyOHLCV` candle, we refer to the indices from `my_ohlcv_fields` (1, 2, and 3) to build our request.

```c
// The ID for our 1-minute candle is its index in the config array.
int my_1min_candle_idx = 0;

// Request the High, Low, and Close fields using their indices from my_ohlcv_fields.
const int fields_to_get[] = {1, 2, 3}; 
const struct trcache_field_request request = {
    .field_indices = fields_to_get,
    .num_fields = 3,
};

// Allocate a batch to store the results.
struct trcache_candle_batch *result_batch = 
    trcache_batch_alloc_on_heap(cache, my_1min_candle_idx, 100, &request);

// Get the 10 most recent candles
if (trcache_get_candles_by_symbol_id_and_offset(cache, aapl_id, my_1min_candle_idx, &request, 0, 10, result_batch) == 0) {
    printf("Retrieved %d candles for AAPL.\n", result_batch->num_candles);
    
    // IMPORTANT: To access the data, use the original indices from your field_definitions array,
    // not the indices from your 'fields_to_get' request array.
    double *high_prices = (double *)result_batch->column_arrays[1]; // Index 1 is 'high'
    double *low_prices = (double *)result_batch->column_arrays[2];  // Index 2 is 'low'
    double *close_prices = (double *)result_batch->column_arrays[3]; // Index 3 is 'close'

    for (int i = 0; i < result_batch->num_candles; i++) {
        printf("  Timestamp: %llu, High: %.2f, Low: %.2f, Close: %.2f\n",
               result_batch->key_array[i], high_prices[i], low_prices[i], close_prices[i]);
    }
}

trcache_batch_free(result_batch);
```

### Step 11: Destroy the Engine

Clean up all resources, stop background threads, and flush any remaining data.

```c
trcache_destroy(cache);
```

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

### SIMD Optimization and Verification

# Evaluation
