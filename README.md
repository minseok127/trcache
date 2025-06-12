# Trcache

Trcache is a high‑performance C library for ingesting real-time trade data and converting it into **column‑oriented OHLCV candle arrays** optimised for analysis.  The library targets multicore machines and relies on lock‑free data structures together with dedicated worker threads to scale with available CPU cores.

## Features

 - **Lock-free pipeline** – Apply, convert, and flush stages run concurrently on dedicated threads without locks.
 	- **Apply** – aggregates trades into row-oriented candles.
 	- **Convert** – reshapes row-oriented candle pages into SIMD-friendly column batches.
 	- **Flush** – hands batches to user-defined callbacks for persistence.
- **Adaptive worker scheduling** – an admin thread monitors throughput and assigns work to worker threads dynamically.
- **Pluggable flushing** – applications supply synchronous or asynchronous callbacks to persist finished batches.
- **Multiple candle types** – time- and tick-based candles can be enabled simultaneously.

## Build

```
make            # build static and shared libraries
```

To generate debug binaries set `BUILD_MODE=debug`:

```
make BUILD_MODE=debug
```

## Running tests

The `tests/` directory contains a suite of unit tests and micro benchmarks.  They can be compiled and executed with

```
make test
```

which will produce several test binaries and run them automatically.

## Basic usage

### 1. Candle and field types

`trcache` exposes enums for all supported candle intervals and for each field contained in a candle:


```c
typedef enum {
    TRCACHE_DAY_CANDLE      = 1 << 0,   /* daily timeframe   */
    TRCACHE_1H_CANDLE       = 1 << 1,   /* 1 hour            */
    TRCACHE_30MIN_CANDLE    = 1 << 2,
    TRCACHE_15MIN_CANDLE    = 1 << 3,
    TRCACHE_5MIN_CANDLE     = 1 << 4,
    TRCACHE_1MIN_CANDLE     = 1 << 5,
    TRCACHE_1SEC_CANDLE     = 1 << 6,
    TRCACHE_100TICK_CANDLE  = 1 << 7,   /* tick based sizes  */
    TRCACHE_50TICK_CANDLE   = 1 << 8,
    TRCACHE_10TICK_CANDLE   = 1 << 9,
    TRCACHE_5TICK_CANDLE    = 1 << 10,
} trcache_candle_type;


typedef enum {
    TRCACHE_START_TIMESTAMP = 1 << 0,
    TRCACHE_START_TRADE_ID  = 1 << 1,
    TRCACHE_OPEN            = 1 << 2,
    TRCACHE_HIGH            = 1 << 3,
    TRCACHE_LOW             = 1 << 4,
    TRCACHE_CLOSE           = 1 << 5,
    TRCACHE_VOLUME          = 1 << 6,
} trcache_candle_field_type;
```
These constants are combined with bitwise OR when specifying which candle types to build or which fields to copy.

### 2. Implement flush operations

`trcache_flush_ops` lets applications decide how completed candle batches are
persisted.  A synchronous flush performs the work inside the callback and returns
`NULL`:

```c
void *sync_flush(trcache *c, trcache_candle_batch *b, void *ctx)
{
    /* write b->open_array … to disk */
    (void)c; (void)ctx;
    return NULL;       /* signals synchronous completion */
}
```

For asynchronous flushing return a handle and implement `is_done` and
`destroy_handle` so the engine can poll for completion:

```c
struct my_handle { int fd; /* opaque */ };

void *async_flush(trcache *c, trcache_candle_batch *b, void *ctx)
{
    struct my_handle *h = start_async_io(b);   /* pseudo API */
    (void)c; (void)ctx;
    return h;          /* non-NULL => async */
}

bool async_is_done(trcache *c, trcache_candle_batch *b, void *handle)
{
    (void)c; (void)b;
    return check_io(((struct my_handle *)handle)->fd);  /* pseudo API */
}

void async_destroy(void *handle, void *ctx)
{
    finish_io(((struct my_handle *)handle)->fd);  /* pseudo API */
    free(handle);
    (void)ctx;
}
```

Supply these callbacks to `trcache_init()`.  When using the asynchronous
variant, also set `ops.is_done` and `ops.destroy_handle`.

### 3. Initialise the engine

```c
struct trcache_flush_ops ops = { .flush = sync_flush };
struct trcache_init_ctx ctx = {
    .num_worker_threads = 4,
    .batch_candle_count_pow2 = 10,  /* 1024 candles per batch */
    .flush_threshold_pow2 = 3,      /* flush after 8 batches */
    .candle_type_flags = TRCACHE_1MIN_CANDLE | TRCACHE_5MIN_CANDLE,
    .flush_ops = ops,
};

struct trcache *cache = trcache_init(&ctx);
if (!cache)
    return 1;
```

Calling `trcache_init()` spawns one admin thread and the specified number of
worker threads automatically.

### 4. Allocate candle batches

`trcache_candle_batch` represents a column‑oriented array of OHLCV candles. These batches serve as flush targets for the engine, or are used when users copy candle data out. Batches can reside either on the heap or on the caller's stack:

```c
/* heap allocation */
struct trcache_candle_batch *hb = trcache_batch_alloc_on_heap(512);

/* stack allocation */
TRCACHE_DEFINE_BATCH_ON_STACK(sb, 512);
```

The heap batch must be released with `trcache_batch_free()` when no longer needed.

### 5. Register and query symbols

```c
int aapl_id = trcache_register_symbol(cache, "AAPL");
const char *name = trcache_lookup_symbol_str(cache, aapl_id); /* "AAPL" */
```

Before inserting real-time trade data into trcache, the user must register the symbol associated with the trade in the system. The `trcache_register_symbol()` API is used to register a symbol string and retrieve the corresponding ID.

### 6. Feed trade data

```c
struct trcache_trade_data td = {
    .timestamp = 1620000000000ULL,
    .trade_id = 1,
    .price = 123.45,
    .volume = 100,
};

trcache_feed_trade_data(cache, &td, aapl_id);
```

The user can buffer real-time trade data into trcache using `trcache_feed_trade_data()`. This data is aggregated into candles by background threads. However, concurrent data pushes to the same symbol by multiple users are not supported, as real-time trade data for a single symbol is typically not received simultaneously from multiple sockets.

### 7. Destroy the engine

```c
trcache_destroy(cache);
```

`trcache_destroy()` stops all worker threads, joins the admin thread and flushes all batches still held in memory before releasing resources.

## Architecture overview

A `trcache` instance manages multiple symbols. Each symbol owns one `trade_data_buffer` and one `candle_chunk_list` per enabled candle type. Incoming trades are pushed into the symbol's buffer; the same trade is aggregated into every candle list for that symbol. The buffered data flows through a three-stage pipeline:

1. **Apply** – trade entries are consumed from the buffer and aggregated into mutable row-oriented candles inside a `candle_chunk`. Each candle_chunk contains pages that store collections of row-oriented candles. Only the most recent candle is mutable; all previous candles within the chunk are immutable.
2. **Convert** – once a row page becomes immutable, it is converted into a column-oriented batch (trcache_candle_batch), where each field is stored in a separate array. All arrays point into a single contiguous, 64-bytes-aligned memory block, allowing the entire batch to be freed with a single free() call or released from the stack if allocated via alloca. The engine guarantees that both the memory block and each array pointer are aligned to 64-bytes to maximize SIMD load/store efficiency.
3. **Flush** – fully converted batches are handed to user‑defined flush callbacks when the per‑list threshold is reached.  Flushes can be synchronous or asynchronous via the `trcache_flush_ops` interface.

Concurrency is handled by an **admin thread** and one or more **worker threads**.  Workers execute pipeline stages while the admin thread schedules work items and balances load based on real‑time throughput statistics.  The scheduler communicates via lock‑free queues ([`scalable_queue`](https://github.com/minseok127/scalable-queue)).

Another lock-free mechanism used in the system is [`atomsnap`](https://github.com/minseok127/atomsnap), which provides grace-period management through a snapshot-based approach. For example, it enables operations such as symbol registration in the symbol table, memory reclamation of row-oriented candle pages that have been fully converted to column-oriented batches, and freeing candle chunks that no longer need to reside in memory after flush—all without blocking readers.
