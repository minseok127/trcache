# 호가(Order Book) 데이터 처리 확장 구현 계획

## 설계 요약

호가 이벤트를 청크 버퍼에 저장 → 워커가 심볼별 상태 객체 갱신 + 원본 이벤트 flush.
캔들 콜백에서 book_state를 읽어 캔들 필드에 호가 정보 포함.
trade_data_buffer를 event_data_buffer로 리네이밍하여 trade/book 양쪽에서 재사용.

## 확정된 설계 결정

- book state_update: GROUP_IN_MEMORY 워커에서 실행
- book event_flush: GROUP_FLUSH 워커에서 실행
- GROUP_BATCH_FLUSH → GROUP_FLUSH로 리네이밍 (batch+trade+book flush 모두 포함)
- 입력 버퍼: event_size/events_per_chunk를 버퍼 구조체에 직접 저장 (trcache 디커플링)
- 리네이밍: trade_data_buffer → event_data_buffer (trade/book 공용이므로)
- candle→record 리네이밍은 이번 범위 제외
- trade/book io_block_size 통일 → TLS free list 공유 (별도 분리 불필요)
- book_state_ops: state_size 제거, init()이 할당+반환, destroy()가 해제 (필수)

---

## Phase 1: trade_data_buffer → event_data_buffer 리네이밍 + 디커플링

**목적**: 버퍼를 trade 전용에서 범용으로 전환. `buf->trc->trade_data_size` 간접 참조를
`buf->event_size` 직접 참조로 변경.

### 1-1. 버퍼 구조체에 event_size, events_per_chunk, event_buf_size 필드 추가

**파일**: `src/include/pipeline/trade_data_buffer.h` → `event_data_buffer.h`로 리네이밍

- `struct trade_data_chunk` → `struct event_data_chunk`
- `struct trade_data_buffer_cursor` → `struct event_data_buffer_cursor`
- `struct trade_data_buffer` → `struct event_data_buffer`
- 새 필드 추가 (Group 2 read-only 영역):
  ```c
  size_t event_size;           // sizeof(유저 이벤트 구조체)
  int    events_per_chunk;     // 청크당 이벤트 수
  size_t event_buf_size;       // 4KiB-aligned 데이터 버퍼 크기
  ```
- `enum trade_chunk_flush_state` → `enum event_chunk_flush_state`
- 매크로: `__get_trd_chunk_ptr` → `__get_evt_chunk_ptr`
- 모든 함수 이름 `trade_data_buffer_*` → `event_data_buffer_*`
- `TRADE_DATA_BUF_ALIGN` → `EVENT_DATA_BUF_ALIGN`

### 1-2. 구현 파일 리네이밍 + 간접 참조 제거

**파일**: `src/pipeline/trade_data_buffer.c` → `event_data_buffer.c`

6개 지점에서 `buf->trc->trade_data_size` → `buf->event_size` 변경:
- `calculate_data_buf_size()`: `tc->trades_per_chunk * tc->trade_data_size` → 인자로 받기
- `event_data_buffer_push()` (3곳):
  - L208: `buf->trc->trades_per_chunk` → `buf->events_per_chunk`
  - L257: `buf->trc->trade_data_buf_size` → `buf->event_buf_size`
  - L293-295: `buf->trc->trade_data_size` → `buf->event_size`
  - L309: `buf->trc->trades_per_chunk` → `buf->events_per_chunk`
- `event_data_buffer_peek()` (2곳):
  - L358: `buf->trc->trade_data_size` → `buf->event_size`
  - L364: `buf->trc->trades_per_chunk` → `buf->events_per_chunk`

`event_data_buffer_init()` 시그니처 변경:
```c
// 기존
struct trade_data_buffer *trade_data_buffer_init(struct trcache *tc, int symbol_id);

// 변경
struct event_data_buffer *event_data_buffer_init(struct trcache *tc,
    int symbol_id, size_t event_size, int events_per_chunk,
    size_t event_buf_size, int num_cursors);
```
init 함수 내부에서 `buf->event_size`, `buf->events_per_chunk`, `buf->event_buf_size` 설정.
`num_cursors` 파라미터로 cursor 개수 지정 (trade: num_candle_configs, book: 1).

### 1-3. 모든 참조 코드 업데이트

**파일**: 다음 파일에서 이름 변경 반영

| 파일 | 변경 내용 |
|------|-----------|
| `src/include/meta/trcache_internal.h` | `#include` 경로, `trade_data_size`/`trades_per_chunk`/`trade_data_buf_size` 필드는 유지 (init 시 계산용) |
| `src/meta/trcache_internal.c` | `trade_data_buffer_*` → `event_data_buffer_*` 함수 호출, `destroy_tls_data`의 chunk 타입 변경 |
| `src/include/meta/symbol_table.h` | `struct trade_data_buffer *trd_buf` → `struct event_data_buffer *trd_buf` |
| `src/meta/symbol_table.c` | `trade_data_buffer_init/destroy` → `event_data_buffer_init/destroy` |
| `src/sched/worker_thread.c` | `trade_data_buffer_*` → `event_data_buffer_*` 함수 호출 |
| `src/sched/admin_thread.c` | 구조체/함수 이름 변경 반영 |
| `Makefile` | 소스 파일명 변경 반영 |

### 1-4. trcache 구조체 정리

**파일**: `src/include/meta/trcache_internal.h`

`trade_data_size`, `trades_per_chunk`, `trade_data_buf_size`는 유지 (init 시 계산, TLS free list 해제 시 사용).
이 필드들이 버퍼에도 복사되지만, TLS destructor에서 free list의 chunk 크기를 알아야 하므로 trcache에도 남겨둠.

### 1-5. trade_io_block_size → feed_block_size 리네이밍

trade/book 공용이므로 `trade_` 접두사 제거.

| 파일 | 변경 |
|------|------|
| `trcache.h` | `trcache_init_ctx.trade_io_block_size` → `feed_block_size`, 주석 수정 |
| `src/meta/trcache_internal.c` | `ctx->trade_io_block_size` → `ctx->feed_block_size` |
| `README.md` | `trade_io_block_size` 언급 변경 |

### 검증
- `make` 빌드 성공
- `make benchmark` 빌드 성공
- 벤치마크 실행하여 기존 동작 확인

---

## Phase 1.5: GROUP_BATCH_FLUSH → GROUP_FLUSH 리네이밍 + flush 관련 변수명 정리

**목적**: flush 그룹이 batch flush 뿐만 아니라 trade flush, book event flush도 포함하므로
정확한 이름으로 변경.

### 1.5-1. enum 및 그룹 이름 변경

**파일**: `src/include/sched/worker_thread.h`

```c
// 기존
enum worker_group_type {
    GROUP_IN_MEMORY,
    GROUP_BATCH_FLUSH
};

// 변경
enum worker_group_type {
    GROUP_IN_MEMORY,
    GROUP_FLUSH
};
```

### 1.5-2. admin_thread.c 그룹 총합 변수명 정리

**파일**: `src/sched/admin_thread.c`

그룹 전체를 지칭하는 변수만 변경 (특정 비트맵/비용 필드는 유지):

| 현재 이름 | 변경 후 | 설명 |
|-----------|---------|------|
| `total_batch_flush_cycles` | `total_flush_cycles` | 그룹 총 사이클 |
| `out_batch_flush_cycles` | `out_flush_cycles` | 출력 파라미터 |
| `num_batch_flush_workers` / `out_num_batch_flush_workers` | `num_flush_workers` / `out_num_flush_workers` | 워커 수 |
| `batch_flush_state` (budget_assign_state) | `flush_state` | 버짓 상태 |

변경하지 않는 것 (캔들 배치 flush 전용):
- `next_batch_flush_bitmaps`, `batch_flush_words_per_worker` — 캔들 배치 flush 비트맵
- `batch_flush_bitmap` (worker_state) — 캔들 배치 flush 전용
- `cost_batch_flush` — 캔들 배치 flush EMA 비용

### 1.5-3. worker_thread.c에서 GROUP_BATCH_FLUSH 참조 변경

**파일**: `src/sched/worker_thread.c`

```c
// 기존
} else if (group == GROUP_BATCH_FLUSH) {

// 변경
} else if (group == GROUP_FLUSH) {
```

### 검증
- `make` 빌드 성공

---

## Phase 2: 캔들 콜백 시그니처에 book_state 추가

**목적**: 캔들 update/init 콜백이 호가 상태를 읽을 수 있도록 파라미터 추가.

### 2-1. 공개 API 변경

**파일**: `trcache.h`

```c
// 기존
typedef struct trcache_candle_update_ops {
    void (*init)(struct trcache_candle_base *c, void *trade_data);
    bool (*update)(struct trcache_candle_base *c, void *trade_data);
} trcache_candle_update_ops;

// 변경
typedef struct trcache_candle_update_ops {
    void (*init)(struct trcache_candle_base *c, void *trade_data,
                 const void *book_state);
    bool (*update)(struct trcache_candle_base *c, void *trade_data,
                   const void *book_state);
} trcache_candle_update_ops;
```

**Breaking change**: 기존 유저 콜백 시그니처가 변경됨. 호가 미사용 시 `book_state`는 NULL.

### 2-2. 콜백 호출부 변경

**파일**: `src/pipeline/candle_chunk_list.c`, `src/pipeline/candle_chunk.c`

3개 호출 지점:
- `candle_chunk.c:327`: `ops->init(first_candle, trade)` → `ops->init(first_candle, trade, book_state)`
- `candle_chunk_list.c:427`: `ops->init(next_candle, trade)` → `ops->init(next_candle, trade, book_state)`
- `candle_chunk_list.c:552`: `ops->update(candle, trade)` → `ops->update(candle, trade, book_state)`

이를 위해 `candle_chunk_list_apply_trade()`에 `book_state` 파라미터 추가:
```c
// 기존
int candle_chunk_list_apply_trade(struct candle_chunk_list *list, void *trade);

// 변경
int candle_chunk_list_apply_trade(struct candle_chunk_list *list,
    void *trade, const void *book_state);
```

`candle_chunk_page_init()` (candle_chunk.c)에도 `book_state` 전달 필요.

### 2-3. 워커/유저 스레드에서 book_state 전달

**파일**: `src/sched/worker_thread.c`

`worker_do_apply()`에서:
```c
// 기존
candle_chunk_list_apply_trade(list, trade_data);

// 변경: symbol_entry에서 book_state 조회
const void *book_state = entry->book_state;  // Phase 3에서 추가될 필드
candle_chunk_list_apply_trade(list, trade_data, book_state);
```

**파일**: `src/meta/trcache_internal.c`

`user_thread_apply_trades()`에서도 동일하게 `book_state` 전달.

### 검증
- `make` 빌드 성공
- 벤치마크에서 book_state=NULL로 동작 확인 (기존 동작 보존)
- validator 수정 후 빌드 확인

---

## Phase 3: book_state 인프라 추가

**목적**: 심볼별 book_state 할당/해제, book_state_ops 저장.

### 3-1. 공개 API 추가

**파일**: `trcache.h`

```c
// 호가 상태 관리 콜백
typedef struct trcache_book_state_ops {
    void* (*init)(void);                              // 유저가 할당+초기화, 포인터 반환
    void  (*update)(void *state, const void *event);  // 워커가 호출
    void  (*destroy)(void *state);                    // 유저가 해제 (필수)
} trcache_book_state_ops;

// 호가 이벤트 flush 콜백 (trade_flush_ops와 동일 패턴)
typedef struct trcache_book_event_flush_ops {
    void (*flush)(trcache *cache, int symbol_id,
        const void *data, int num_events, void *ctx);
    bool (*is_done)(trcache *cache, const void *data, void *ctx);
    void *ctx;
} trcache_book_event_flush_ops;

// trcache_init_ctx에 추가
typedef struct trcache_init_ctx {
    // ... 기존 필드 ...
    struct trcache_book_state_ops book_state_ops;       // .init == NULL이면 미사용
    struct trcache_book_event_flush_ops book_event_flush_ops;  // .flush == NULL이면 미사용
    size_t book_event_size;                             // sizeof(유저의 book event)
} trcache_init_ctx;

// 호가 이벤트 feed
int trcache_feed_book_data(struct trcache *cache,
    const void *book_event, int symbol_id);
```

**참고**: `book_io_block_size`는 별도로 두지 않음. `trade_io_block_size`를 `feed_block_size`로
리네이밍하여 trade/book 공용으로 사용. TLS free list도 공유.

### 3-2. symbol_entry에 book_state, book_buf 추가

**파일**: `src/include/meta/symbol_table.h`

```c
struct symbol_entry {
    struct candle_chunk_list *candle_chunk_list_ptrs[MAX_CANDLE_TYPES];
    struct event_data_buffer *trd_buf;
    struct event_data_buffer *book_buf;  // 새로 추가 (NULL이면 미사용)
    void *book_state;                    // 새로 추가 (NULL이면 미사용)
    char *symbol_str;
    int id;
};
```

### 3-3. trcache 구조체에 book 관련 필드 추가

**파일**: `src/include/meta/trcache_internal.h`

```c
struct trcache {
    // ... 기존 필드 (Group 4: Read-Only) ...
    struct trcache_book_state_ops book_state_ops;
    struct trcache_book_event_flush_ops book_event_flush_ops;
    size_t book_event_size;
    int    book_events_per_chunk;
    size_t book_event_buf_size;
    bool   book_enabled;                // book_state_ops.init != NULL
};
```

### 3-4. 초기화/해제에 book 처리 추가

**파일**: `src/meta/trcache_internal.c`

`trcache_init()`:
- `book_state_ops` 복사
- `book_enabled = (ctx->book_state_ops.init != NULL)` 설정
- `book_enabled`이면:
  - `book_event_size` 검증 (> 0)
  - `book_events_per_chunk = io_block_size / book_event_size` (trade와 동일 io_block_size 사용)
  - `book_event_buf_size = ALIGN_UP(book_events_per_chunk * book_event_size, 4096)`

`trcache_destroy()`:
- `book_event_flush_ops` 설정 시 모든 심볼의 book_buf finalize

**파일**: `src/meta/symbol_table.c`

`init_symbol_entry()`:
- `book_enabled`이면:
  - `event_data_buffer_init(tc, id, book_event_size, book_events_per_chunk, book_event_buf_size, 1)` 호출
  - `entry->book_state = tc->book_state_ops.init()` 호출

`symbol_table_destroy()`:
- `book_state` 해제: `book_state_ops.destroy(entry->book_state)`
- `book_buf` 해제: `event_data_buffer_destroy(entry->book_buf)`

### 3-5. feed_book_data 구현

**파일**: `src/meta/trcache_internal.c`

```c
int trcache_feed_book_data(struct trcache *tc,
    const void *data, int symbol_id)
{
    // get_tls_data_or_create(), symbol lookup
    // event_data_buffer_push(entry->book_buf, data, &tls->local_free_list, ...)
    // trade와 동일한 io_block_size → 동일한 free list 공유
}
```

TLS free list는 trade와 공유 (io_block_size 통일됨).
별도 `local_book_free_list`는 불필요.

### 검증
- `make` 빌드 성공
- `book_state_ops.init == NULL` (미사용) 시 기존 동작 보존 확인

---

## Phase 4: book 워커 태스크 추가

**목적**: 워커가 book 이벤트를 처리하고 flush할 수 있도록 비트맵 + 실행 로직 추가.

### 4-1. 비트맵 매크로 추가

**파일**: `src/include/sched/worker_thread.h`

```c
// book_update_bitmap — 1 bit per symbol (GROUP_IN_MEMORY에서 스캔)
#define BOOK_UPDATE_BIT(sym_idx)          (sym_idx)
#define BOOK_UPDATE_BIT_TO_SYM_IDX(bit)  (bit)

// book_event_flush_bitmap — 1 bit per symbol (GROUP_FLUSH에서 스캔)
#define BOOK_FLUSH_BIT(sym_idx)           (sym_idx)
#define BOOK_FLUSH_BIT_TO_SYM_IDX(bit)   (bit)
```

### 4-2. worker_state에 book 비트맵 추가

**파일**: `src/include/sched/worker_thread.h`

```c
struct worker_state {
    // ... 기존 ...
    uint64_t *book_update_bitmap;          // GROUP_IN_MEMORY에서 스캔
    uint64_t *book_event_flush_bitmap;     // GROUP_FLUSH에서 스캔
};
```

### 4-3. symbol_table에 book ownership flags 추가

**파일**: `src/include/meta/symbol_table.h`

```c
struct symbol_table {
    // ... 기존 ...
    _Atomic(int) *book_update_ownership_flags;       // [sym_idx]
    _Atomic(int) *book_event_flush_ownership_flags;  // [sym_idx]
};
```

### 4-4. 워커 실행 함수 추가

**파일**: `src/sched/worker_thread.c`

```c
// book state update: 버퍼에서 이벤트 읽기 → book_state_ops.update() 호출
static void worker_do_book_update(struct trcache *cache,
    struct symbol_entry *entry)
{
    struct event_data_buffer *buf = entry->book_buf;
    struct event_data_buffer_cursor *cur;
    void *array, *event;
    int count;

    cur = event_data_buffer_acquire_cursor(buf, 0);  // cursor 1개 (index 0)
    if (cur == NULL) return;

    while (event_data_buffer_peek(buf, cur, &array, &count) && count > 0) {
        for (int i = 0; i < count; i++) {
            event = (uint8_t *)array + (i * buf->event_size);
            cache->book_state_ops.update(entry->book_state, event);
        }
        event_data_buffer_consume(buf, cur, count);
    }

    event_data_buffer_release_cursor(cur);
}

// book event flush: trade flush와 동일 패턴
static void worker_do_book_event_flush(struct trcache *cache,
    struct symbol_entry *entry)
{
    event_data_buffer_flush_full_chunks(
        entry->book_buf, &cache->book_event_flush_ops);
    // EMA 업데이트
}
```

비트맵 스캔 함수 (`process_book_update_word`, `worker_run_book_update_tasks` 등)는
trade_flush의 패턴을 그대로 따름 (심볼당 1비트).

### 4-5. 워커 메인 루프에 book 태스크 통합

**파일**: `src/sched/worker_thread.c`

```c
if (group == GROUP_IN_MEMORY) {
    work_done = worker_run_in_memory_tasks(cache, state);
    if (cache->book_enabled)
        work_done |= worker_run_book_update_tasks(cache, state);  // 추가
} else if (group == GROUP_FLUSH) {
    work_done = worker_run_batch_flush_tasks(cache, state);
    work_done |= worker_run_trade_flush_tasks(cache, state);
    if (cache->book_enabled)
        work_done |= worker_run_book_event_flush_tasks(cache, state);  // 추가
}
```

### 4-6. worker_state_init/destroy에 book 비트맵 할당/해제

**파일**: `src/sched/worker_thread.c`

`worker_state_init()`: book_update_bitmap, book_event_flush_bitmap 할당 (max_symbols 비트)
`worker_state_destroy()`: free

### 4-7. symbol_table_init/destroy에 book ownership flags 할당/해제

**파일**: `src/meta/symbol_table.c`

### 검증
- `make` 빌드 성공
- 단일 심볼에 book 이벤트 feed → book_state 갱신 확인
- book_state 읽기와 동시에 book event feed (동시성 테스트)

---

## Phase 5: admin 스레드에 book 파이프라인 모니터링 추가

**목적**: admin이 book 태스크의 수요를 추적하고 비트맵에 할당.

### 5-1. admin_state에 book 관련 필드 추가

**파일**: `src/include/sched/admin_thread.h`

```c
struct admin_state {
    // ... 기존 ...
    // book update 비용/수요 (심볼당)
    double *book_update_costs;             // [sym_idx]
    uint64_t *book_chunk_fill_rates;       // [sym_idx]
    uint64_t *book_fill_timestamps;        // [sym_idx]
    double *book_event_flush_costs;        // [sym_idx]

    // book 비트맵 버퍼
    uint64_t *next_book_update_bitmaps;
    uint64_t *next_book_event_flush_bitmaps;
    uint64_t *current_book_update_bitmaps;
    uint64_t *current_book_event_flush_bitmaps;
    size_t book_update_words_per_worker;
    size_t book_event_flush_words_per_worker;
};
```

### 5-2. book 수요를 calculate_total_cycle_needs에 반영

**파일**: `src/sched/admin_thread.c`

`calculate_total_cycle_needs()` (변수명은 Phase 1.5에서 이미 정리됨):
```c
// book update 수요 → total_im_cycles에 추가
for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
    total_im_cycles +=
        (double)admin->book_chunk_fill_rates[sym_idx]
        * admin->book_update_costs[sym_idx];
}

// book event flush 수요 → total_flush_cycles에 추가
for (int sym_idx = 0; sym_idx < num_symbols; sym_idx++) {
    total_flush_cycles +=
        (double)admin->book_chunk_fill_rates[sym_idx]
        * admin->book_event_flush_costs[sym_idx];
}
```

### 5-3. book 태스크 비트맵 할당 함수 추가

**파일**: `src/sched/admin_thread.c`

`assign_book_update_tasks()`: im_state 버짓 그룹에 할당
`assign_book_event_flush_tasks()`: flush_state 버짓 그룹에 할당
패턴은 `assign_trade_flush_tasks()`와 동일.

### 5-4. publish에 book 비트맵 포함

**파일**: `src/sched/admin_thread.c`

`publish_new_assignments_to_workers()`:
- GROUP_IN_MEMORY 워커: book_update_bitmap memcmp/memcpy 추가
- GROUP_FLUSH 워커: book_event_flush_bitmap memcmp/memcpy 추가

### 5-5. update_global_memory_stats에 book_buf 메모리 합산

**파일**: `src/sched/admin_thread.c`

```c
if (entry->book_buf) {
    total_mem += mem_get_atomic(&entry->book_buf->memory_usage.value);
}
```

### 검증
- `make` 빌드 성공
- admin 스레드가 book 태스크를 워커에 분배하는지 로그 확인
- book 이벤트 고빈도 feed 시 워커 리밸런싱 동작 확인

---

## Phase 6: Makefile + 헤더 가드 정리

**파일**: `Makefile`

- `trade_data_buffer.c` → `event_data_buffer.c` 반영
- 새 소스 파일 추가 (있다면)

**파일**: 리네이밍된 헤더 가드

- `TRADE_DATA_BUFFER_H` → `EVENT_DATA_BUFFER_H`

---

## Phase 7: validator 수정

**파일**: `validator/` 내 소스

- `trcache_candle_update_ops` 콜백 시그니처에 `const void *book_state` 추가 (무시)
- 빌드 확인

---

## 의존 관계 요약

```
Phase 1   (event_data_buffer 리네이밍)
    ↓
Phase 1.5 (GROUP_FLUSH 리네이밍 + 변수명 정리)
    ↓
Phase 2   (콜백 시그니처)
    ↓
Phase 3   (book_state 인프라)  ← Phase 1 의존 (event_data_buffer 사용)
    ↓
Phase 4   (워커 태스크)        ← Phase 2,3 의존
    ↓
Phase 5   (admin 모니터링)     ← Phase 4 의존
    ↓
Phase 6   (Makefile 정리)      ← 전체 의존
    ↓
Phase 7   (validator 수정)     ← Phase 2 의존
```

## 위험 요소

1. **Phase 1 리네이밍 범위**: trade_data_buffer를 참조하는 모든 파일을 빠짐없이 변경해야 함.
   grep으로 `trade_data_buffer`, `trade_data_chunk`, `trd_buf`, `__get_trd_chunk_ptr` 전수 검색 필요.

2. **콜백 시그니처 변경은 breaking change**: validator, 벤치마크, 모든 유저 코드 수정 필요.

3. **event_data_buffer_flush_full_chunks의 flush ops 타입**: trade_flush_ops와
   book_event_flush_ops는 시그니처가 동일하지만 별도 타입. 내부적으로 flush 함수를
   호출할 때 void* 캐스팅이 필요하거나, 공통 flush ops 타입을 정의해야 함.

4. **event_data_buffer의 cursor 수**: trade 버퍼는 cursor가 num_candle_configs개,
   book 버퍼는 cursor가 1개. `cursor_arr[MAX_CANDLE_TYPES]`를 고정 크기로 유지하되
   `num_cursor`를 init 시 설정하는 방식으로 처리.

5. **TLS free list 공유 전제**: trade와 book의 io_block_size가 동일해야 함.
   trcache_init()에서 둘 다 같은 io_block_size로 계산되도록 보장.
