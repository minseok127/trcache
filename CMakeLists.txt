cmake_minimum_required(VERSION 3.15)
project(trcache C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(TRCACHE_SOURCES
    src/concurrent/atomsnap.c
    src/concurrent/scalable_queue.c
    src/meta/symbol_table.c
    src/meta/trcache_candle_batch.c
    src/meta/trcache_internal.c
    src/pipeline/candle_chunk.c
    src/pipeline/candle_chunk_index.c
    src/pipeline/candle_chunk_list.c
    src/pipeline/event_data_buffer.c
    src/sched/admin_thread.c
    src/sched/worker_thread.c
    src/utils/hash_table.c
    src/utils/hash_table_callbacks.c
)

add_library(trcache STATIC ${TRCACHE_SOURCES})

target_include_directories(trcache PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src/include
)

if(MSVC)
    target_compile_options(trcache PRIVATE /W4 /O2)
    target_compile_definitions(trcache PRIVATE
        _CRT_SECURE_NO_WARNINGS
    )
else()
    target_compile_options(trcache PRIVATE
        -Wall -Wextra -O2 -fPIC -march=native
    )
    target_link_libraries(trcache PRIVATE pthread)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(trcache PRIVATE TRCACHE_DEBUG)
    if(MSVC)
        target_compile_options(trcache PRIVATE /Od /Zi)
    else()
        target_compile_options(trcache PRIVATE -O0 -g)
    endif()
endif()
