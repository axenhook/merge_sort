#include <alloc.h>
#include <barrier.h>
#include <defs.h>
#include <mram.h>
#include <mutex.h>
#include <perfcounter.h>

#include <stdint.h>
#include <string.h>
#include "request.h"

//#define SEQREAD_CACHE_SIZE 256
#include "seqread.h"

#ifdef TRACE
STDOUT_BUFFER_INIT(256);
#endif

#define WCACHE_SIZE 256 

BARRIER_INIT(barrier, NR_TASKLETS);
MUTEX_INIT(mutex_responses);

__host algo_request_t DPU_REQUEST_VAR;
__host algo_stats_t DPU_STATS_VAR;

// sequential reader 
seqreader_t sr1[NR_TASKLETS], sr2[NR_TASKLETS];

// write cache
__dma_aligned tuple_t wcache[NR_TASKLETS][WCACHE_SIZE];
uint32_t wcache_index[NR_TASKLETS] = {0};

uintptr_t data_begin = (uintptr_t)DPU_MRAM_HEAP_POINTER;
uintptr_t tmp_begin = (uintptr_t)DPU_MRAM_HEAP_POINTER + MRAM_SIZE + MRAM_SIZE;

void flush_cache(uint8_t tid, __mram_ptr tuple_t *wmem, uint32_t * mram_index) {

    if(wcache_index[tid] == 0) return;
    mram_write(wcache[tid], &wmem[*mram_index], sizeof(tuple_t) * wcache_index[tid]);
    *mram_index += wcache_index[tid];
    wcache_index[tid] = 0;
}

void cache_write(uint8_t tid, const tuple_t * tp, 
        __mram_ptr tuple_t *wmem, uint32_t * mram_index) {

    // if cache is full, first write it to mram
    if(wcache_index[tid] == WCACHE_SIZE) {
    flush_cache(tid, wmem, mram_index);
    }

    wcache[tid][wcache_index[tid]] = *tp;
    wcache_index[tid]++;
}

void merge(__mram_ptr tuple_t *a, uint32_t left, uint32_t mid, uint32_t right, __mram_ptr tuple_t *tmp) {
    uint32_t i = left;
    uint32_t j = mid;
    uint32_t k = left;

    // initialize the sequential readers to read from address in MRAM
    tuple_t *ti = seqread_seek(&a[left], &sr1[me()]);
    tuple_t *tj = seqread_seek(&a[mid], &sr2[me()]);

    while (i < mid && j < right) {
        if (ti->key < tj->key) {
            cache_write(me(), ti, tmp, &k);
            ti = seqread_get(ti, sizeof(tuple_t), &sr1[me()]);
            i++;
        } else {
            cache_write(me(), tj, tmp, &k);
            tj = seqread_get(tj, sizeof(tuple_t), &sr2[me()]);
            j++;
        }
    }

    while (i < mid) {
        cache_write(me(), ti, tmp, &k);
        ti = seqread_get(ti, sizeof(tuple_t), &sr1[me()]);
        i++;
    }

    while (j < right) {
        cache_write(me(), tj, tmp, &k);
        tj = seqread_get(tj, sizeof(tuple_t), &sr2[me()]);
        j++;
    }

    flush_cache(me(), tmp, &k);
}

void merge_sort(__mram_ptr tuple_t *a, uint32_t len, __mram_ptr tuple_t *tmp) {
    if (len <= 1)
        return;

    uint32_t toggle = 0;
    __mram_ptr tuple_t *src, *dst;
    for (uint32_t width = 1; width < len; width <<= 1) {
        if (toggle & 1) {
            src = tmp;
            dst = a;
        }
        else {
            src = a;
            dst = tmp;
        }
        //clock_t t = clock();
        for (uint32_t i = 0; i < len; i += (width << 1)) {
            uint32_t mid = i + width;
            if (mid > len)
                mid = len;

            uint32_t right = mid + width;
            if (right > len)
                right = len;

            merge(src, i, mid, right, dst);
        }
        //t = clock() - t;
        //printf("width: %d, time: %f ms\n", width, (float)t * 1000 / CLOCKS_PER_SEC);
        toggle++;
    }

    if (toggle & 1)
        memcpy(a, tmp, len * sizeof(tuple_t));
}

uint32_t merge_join(__mram_ptr tuple_t *r, __mram_ptr tuple_t *s, uint32_t num_r, uint32_t num_s) {//, void *output) {
    uint32_t i = 0, j = 0, matches = 0;

    tuple_t *ti = seqread_seek(r, &sr1[me()]);
    tuple_t *tj = seqread_seek(s, &sr2[me()]);
    while (i < num_r && j < num_s) {
        if (ti->key < tj->key) {
            i++;
            ti = seqread_get(ti, sizeof(tuple_t), &sr1[me()]);
        }
        else if (ti->key > tj->key) {
            j++;
            tj = seqread_get(tj, sizeof(tuple_t), &sr2[me()]);
        }
        else {
            matches++;
            j++;
        }
    }

    return matches;
}

int main()
{
    if (me() == 0) {
        perfcounter_config(COUNT_CYCLES, true);
        mem_reset();
        memset(&DPU_STATS_VAR, 0, sizeof(DPU_STATS_VAR));
    }

    barrier_wait(&barrier);

    // allocate two sequential readers to read from MRAM in WRAM
    seqread_init(seqread_alloc(), 0, &sr1[me()]);
    seqread_init(seqread_alloc(), 0, &sr2[me()]);

    uintptr_t tmp_offset = me() * MRAM_SIZE_PER_TASKLET;
    uintptr_t data_offset = me() * MRAM_SIZE_PER_TASKLET << 1;
    uintptr_t r_data = data_begin + data_offset;
    uintptr_t s_data = r_data + MRAM_SIZE_PER_TASKLET;

    merge_sort((__mram_ptr void *)r_data, TUPLES_NUM_PER_TASKLET, (__mram_ptr void *)(tmp_begin + tmp_offset));
    merge_sort((__mram_ptr void *)s_data, TUPLES_NUM_PER_TASKLET, (__mram_ptr void *)(tmp_begin + tmp_offset));

    uint32_t matches = merge_join((__mram_ptr void *)r_data, (__mram_ptr void *)s_data, TUPLES_NUM_PER_TASKLET, TUPLES_NUM_PER_TASKLET);


    DPU_STATS_VAR.nb_results[me()] = matches;
    DPU_STATS_VAR.exec_time = perfcounter_get();

    return 0;
}
