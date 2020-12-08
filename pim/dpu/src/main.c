/*
 * Copyright (c) 2014-2017 - uPmem
 */

/*
 * Index search on DPUs: main routine.
 *
 * Each tasklet executes the same main function that
 * gets a search request from the system mailbox and performs
 * the requested operation if it has the proper fragments
 * to operate; the meaning of "proper fragments" depending
 * on the request.
 *
 * Arguments to every request are:
 *  - First, the number N of words to the request
 *  - Followed by N word identifiers, N < MAX_REQUESTED_WORDS
 *
 * This file can be compiled with active traces on, by setting the
 * TRACE macro.
 */

#include <alloc.h>
#include <barrier.h>
#include <defs.h>
#include <mram.h>
#include <mutex.h>
#include <perfcounter.h>

#include <stdint.h>
#include <string.h>
#include "request.h"

#ifdef TRACE
STDOUT_BUFFER_INIT(256);
#endif

BARRIER_INIT(barrier, NR_TASKLETS);
MUTEX_INIT(mutex_responses);

#define TUPLES_NUM_IN_CACHE (1024/sizeof(tuple_t))
__dma_aligned tuple_t tuples_cache[TUPLES_NUM_IN_CACHE];

__host algo_request_t DPU_REQUEST_VAR;
__host algo_stats_t DPU_STATS_VAR;

uintptr_t data_begin = (uintptr_t)DPU_MRAM_HEAP_POINTER;
uintptr_t tmp_begin = (uintptr_t)DPU_MRAM_HEAP_POINTER + MRAM_SIZE + MRAM_SIZE;

void merge(__mram_ptr tuple_t *a, int left, int mid, int right, __mram_ptr tuple_t *tmp) {
	int i = left;
	int j = mid;
	int k = left;

	while (i < mid && j < right) {
		tuple_t ai, aj;
		mram_read(&a[i], &ai, sizeof(tuple_t));
		mram_read(&a[j], &aj, sizeof(tuple_t));
		if (ai.key < aj.key) {
			mram_write(&ai, &tmp[k], sizeof(tuple_t));
			k++;
			i++;
		} else {
			mram_write(&aj, &tmp[k], sizeof(tuple_t));
			k++;
			j++;
		}
	}

	while (i < mid) {
		tuple_t ai;
		mram_read(&a[i], &ai, sizeof(tuple_t));
		mram_write(&ai, &tmp[k], sizeof(tuple_t));
		k++;
		i++;
	}

	while (j < right) {
		tuple_t aj;
		mram_read(&a[j], &aj, sizeof(tuple_t));
		mram_write(&aj, &tmp[k], sizeof(tuple_t));
		k++;
		j++;
	}

//	memcpy(a + left, tmp + left, sizeof(tuple_t) * (right - left));
}

void merge_sort(__mram_ptr tuple_t *a, int len, __mram_ptr tuple_t *tmp) {
	if (len <= 1)
		return;

	int toggle = 0;
	__mram_ptr tuple_t *src, *dst;
	for (int width = 1; width < len; width <<= 1) {
		if (toggle & 1) {
			src = tmp;
			dst = a;
		}
		else {
			src = a;
			dst = tmp;
		}
		//clock_t t = clock();
		for (int i = 0; i < len; i += (width << 1)) {
			int mid = i + width;
			if (mid > len)
				mid = len;

			int right = mid + width;
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

int merge_join(__mram_ptr tuple_t *tuplesR, __mram_ptr tuple_t *tuplesS, int numR, int numS) {//, void *output) {
	int i = 0, j = 0, matches = 0;

	while (i < numR && j < numS) {
		tuple_t ai, aj;
		mram_read(&tuplesR[i], &ai, sizeof(tuple_t));
		mram_read(&tuplesS[j], &aj, sizeof(tuple_t));
		if (ai.key < aj.key)
			i++;
		else if (ai.key > aj.key)
			j++;
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
