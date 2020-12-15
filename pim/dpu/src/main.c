#include <alloc.h>
#include <barrier.h>
#include <defs.h>
#include <mram.h>
#include <mutex.h>
#include <perfcounter.h>
#include <stdio.h>

#include <stdint.h>
#include <string.h>
#include "request.h"

#ifdef TRACE
STDOUT_BUFFER_INIT(256);
#endif

BARRIER_INIT(barrier, NR_TASKLETS);
MUTEX_INIT(mutex_responses);

__host algo_request_t DPU_REQUEST_VAR;
__host algo_stats_t DPU_STATS_VAR;

uintptr_t data_begin = (uintptr_t)DPU_MRAM_HEAP_POINTER;
uintptr_t tmp_begin = (uintptr_t)DPU_MRAM_HEAP_POINTER + MRAM_SIZE + MRAM_SIZE;

#define CACHE_SIZE       1024
#define TUPLES_PER_CACHE ((uint32_t)CACHE_SIZE / sizeof(tuple_t))
#define INVALID_POS      ((uint32_t)-1)
#define POS_SHIFT        3 // log2(sizeof(tuple_t))

typedef struct {
    char *buffer;
    uint32_t cnt;
    uint32_t begin_pos;
    uint32_t pos;
} cache_t;

typedef struct {
    cache_t cache;
    uint32_t pos_mask;
    uint32_t pos_shift;
    bool     is_rd_cache;
    uint32_t memory_size;
    __mram_ptr char    *memory;
} cache_mgr_t;

cache_mgr_t cache[NR_TASKLETS][3];

void init_cache(cache_mgr_t *mgr, __mram_ptr void *memory, uint32_t memory_size, bool is_rd_cache) {
    //assert(memory_size % CACHE_SIZE == 0);

    memset(mgr, 0, sizeof(cache_mgr_t));
    
    mgr->pos_mask = TUPLES_PER_CACHE - 1;
    //assert((mgr->pos_mask & TUPLES_PER_CACHE) == 0);

    mgr->pos_shift = POS_SHIFT;
    mgr->memory_size = memory_size;
    mgr->is_rd_cache = is_rd_cache;
    mgr->memory = memory;

    mgr->cache.buffer = mem_alloc(CACHE_SIZE); // WRAM alloc
    //assert(mgr->cache.buffer != NULL);

    mgr->cache.pos = INVALID_POS;
    mgr->cache.begin_pos = INVALID_POS;
    mgr->cache.cnt = 0;

    //printf("pos_mask: 0x%x, member_num: %u, pos_shift: %u, memory_size: %u\n",
      //     mgr->pos_mask, TUPLES_PER_CACHE, mgr->pos_shift, mgr->memory_size);
}

void reset_cache(cache_mgr_t *mgr, __mram_ptr void *memory, uint32_t memory_size, bool is_rd_cache) {
    //assert(memory_size % CACHE_SIZE == 0);

    mgr->memory_size = memory_size;
    mgr->is_rd_cache = is_rd_cache;
    mgr->memory = memory;  // maybe new memory

    mgr->cache.pos = INVALID_POS;
    mgr->cache.begin_pos = INVALID_POS;
    mgr->cache.cnt = 0;
}

void flush_cache(cache_mgr_t *mgr) {
    if (mgr->is_rd_cache)
        return;

    if (mgr->cache.cnt) {
        //printf("flush: memory: %p, cnt: %u, pos: %u\n", mgr->memory, mgr->cache.cnt, mgr->cache.begin_pos);
        mram_write(mgr->cache.buffer, &mgr->memory[mgr->cache.begin_pos << mgr->pos_shift], CACHE_SIZE);
    }
}

void *get_member(cache_mgr_t *mgr, uint32_t pos) {
    uint32_t begin_pos = pos & ~mgr->pos_mask;

    //hit
    if (mgr->cache.begin_pos == begin_pos) {
        if (mgr->cache.pos != pos)
            mgr->cache.pos = pos;

        mgr->cache.cnt++;
        return &mgr->cache.buffer[(pos & mgr->pos_mask) << mgr->pos_shift];
    }

    // first use
    if (mgr->cache.begin_pos == INVALID_POS) {
        if (mgr->is_rd_cache)
            memcpy(mgr->cache.buffer, &mgr->memory[begin_pos << mgr->pos_shift], CACHE_SIZE);

        mgr->cache.begin_pos = begin_pos;
        mgr->cache.pos = pos;
        mgr->cache.cnt = 1;

        return &mgr->cache.buffer[(pos & mgr->pos_mask) << mgr->pos_shift];
    }

    // reuse
    // printf("memory: %p, cnt: %u, begin_pos: %u, pos_cache: %u, new_begin_pos: %u, is_rd_cache: %d\n",
    //        mgr->memory, mgr->cache.cnt, mgr->cache.begin_pos, mgr->cache.pos-mgr->cache.begin_pos, begin_pos, mgr->is_rd_cache);

    if (mgr->is_rd_cache)
        mram_read(&mgr->memory[begin_pos << mgr->pos_shift], mgr->cache.buffer, CACHE_SIZE);
    else
        mram_write(mgr->cache.buffer, &mgr->memory[mgr->cache.begin_pos << mgr->pos_shift], CACHE_SIZE);

    mgr->cache.begin_pos = begin_pos;
    mgr->cache.pos = pos;
    mgr->cache.cnt = 1;

    return &mgr->cache.buffer[(pos & mgr->pos_mask) << mgr->pos_shift];
}

void merge(cache_mgr_t *a, cache_mgr_t *b, uint32_t left, uint32_t mid, uint32_t right, cache_mgr_t *tmp) {
    uint32_t i = left;
    uint32_t j = mid;
    uint32_t k = left;
    tuple_t *ai, *aj, *tmpk;

    while (i < mid && j < right) {
        ai = get_member(a, i);
        aj = get_member(b, j);
        tmpk = get_member(tmp, k);
        if (ai->key < aj->key) {
            *tmpk = *ai;
            i++;
            k++;
        }
        else {
            *tmpk = *aj;
            j++;
            k++;
        }
    }

    while (i < mid) {
        ai = get_member(a, i++);
        tmpk = get_member(tmp, k++);
        *tmpk = *ai;
    }

    while (j < right) {
        aj = get_member(b, j++);
        tmpk = get_member(tmp, k++);
        *tmpk = *aj;
    }
}

// non-recursive
void merge_sort(__mram_ptr tuple_t *a, uint32_t len, __mram_ptr tuple_t *tmp) {
    if (len <= 1)
        return;

    uint32_t toggle = 0;
    cache_mgr_t *srca, *srcb, *dst;
    srca = &cache[me()][0];
    srcb = &cache[me()][1];
    dst = &cache[me()][2];
    for (uint32_t width = 1; width < len; width <<= 1) {
        if (toggle & 1) {
            reset_cache(srca, tmp, len * sizeof(tuple_t), true);
            reset_cache(srcb, tmp, len * sizeof(tuple_t), true);
            reset_cache(dst, a, len * sizeof(tuple_t), false);
        }
        else {
            reset_cache(srca, a, len * sizeof(tuple_t), true);
            reset_cache(srcb, a, len * sizeof(tuple_t), true);
            reset_cache(dst, tmp, len * sizeof(tuple_t), false);
        }
        //clock_t t = clock();
        for (uint32_t i = 0; i < len; i += (width << 1)) {
            uint32_t mid = i + width;
            if (mid > len)
                mid = len;

            uint32_t right = mid + width;
            if (right > len)
                right = len;

            merge(srca, srcb, i, mid, right, dst);
        }
        //t = clock() - t;
        //printf("width: %d, time: %f ms\n", width, (float)t * 1000 / CLOCKS_PER_SEC);
        flush_cache(dst);
        toggle++;
    }

    if (toggle & 1)
        memcpy(a, tmp, len * sizeof(tuple_t));
}

uint32_t merge_join(cache_mgr_t *r, cache_mgr_t *s, uint32_t num_r, uint32_t num_s) {//, void *output) {
    uint32_t i = 0, j = 0, matches = 0;
    tuple_t *ri, *sj;

    while (i < num_r && j < num_s) {
        ri = get_member(r, i);
        sj = get_member(s, j);
        if (ri->key < sj->key)
            i++;
        else if (ri->key > sj->key)
            j++;
        else {
            matches++;
            j++;
        }
    }

    return matches;
}

uint32_t tuples_sorted(cache_mgr_t *r0, cache_mgr_t *r1, uint32_t num) {
    tuple_t *t0, *t1;
    for (uint32_t i = 0; i < num - 1; i++) {
        t0 = get_member(r0, i);
        t1 = get_member(r1, i+1);
        if (t0->key + 1 != t1->key) {
            printf("tasklet:%u, m0: %p, c0: %p, m1: %p, c1: %p\n", me(), r0->memory, r0->cache.buffer, r1->memory, r1->cache.buffer);
            printf("tasklet:%u, i: %u, key: %u.%p, i+1: %u, key: %u.%p\n", me(), i, t0->key, &t0->key, i+1, t1->key, &t1->key);
            return i;
        }
    }
    return 0xFFFFFFFF;
}

int main()
{
    if (me() == 0) {
        perfcounter_config(COUNT_CYCLES, true);
        mem_reset();
        memset(&DPU_STATS_VAR, 0, sizeof(DPU_STATS_VAR));
    }

    barrier_wait(&barrier);

    uintptr_t tmp_offset = me() * MRAM_SIZE_PER_TASKLET;
    uintptr_t data_offset = me() * MRAM_SIZE_PER_TASKLET << 1;
    uintptr_t r_data = data_begin + data_offset;
    uintptr_t s_data = r_data + MRAM_SIZE_PER_TASKLET;

    init_cache(&cache[me()][0], (__mram_ptr void *)r_data, TUPLES_NUM_PER_TASKLET * sizeof(tuple_t), true);
    init_cache(&cache[me()][1], (__mram_ptr void *)s_data, TUPLES_NUM_PER_TASKLET * sizeof(tuple_t), true);
    init_cache(&cache[me()][2], (__mram_ptr void *)(tmp_begin + tmp_offset), TUPLES_NUM_PER_TASKLET * sizeof(tuple_t), false);

    merge_sort((__mram_ptr void *)r_data, TUPLES_NUM_PER_TASKLET, (__mram_ptr void *)(tmp_begin + tmp_offset));
    merge_sort((__mram_ptr void *)s_data, TUPLES_NUM_PER_TASKLET, (__mram_ptr void *)(tmp_begin + tmp_offset));

    reset_cache(&cache[me()][0], (__mram_ptr void *)r_data, TUPLES_NUM_PER_TASKLET * sizeof(tuple_t), true);
    reset_cache(&cache[me()][1], (__mram_ptr void *)s_data, TUPLES_NUM_PER_TASKLET * sizeof(tuple_t), true);

    uint32_t matches = merge_join(&cache[me()][0], &cache[me()][1], TUPLES_NUM_PER_TASKLET, TUPLES_NUM_PER_TASKLET);
    //uint32_t matches = tuples_sorted(&cache[me()][0], &cache[me()][1], TUPLES_NUM_PER_TASKLET);

    DPU_STATS_VAR.nb_results[me()] = matches;
    DPU_STATS_VAR.exec_time = perfcounter_get();

    return 0;
}
