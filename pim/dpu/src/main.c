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

#include "matcher.h"
#include "request.h"
#include "trace.h"

#ifdef TRACE
STDOUT_BUFFER_INIT(256);
#endif

BARRIER_INIT(barrier, NR_TASKLETS);
MUTEX_INIT(mutex_responses);

__mram_noinit response_t DPU_RESPONSES_VAR[MAX_REQUEST_NUM][MAX_RESPONSES];

__host algo_request_t DPU_REQUEST_VAR[MAX_REQUEST_NUM];
__host algo_stats_t DPU_STATS_VAR;

#ifdef STATS_ON
extern uint32_t get_bytes_read(uint32_t, uint32_t);

#define UPDATE_BYTES_WRITTEN                                                                                                     \
    do {                                                                                                                         \
        mutex_lock(mutex_responses);                                                                                             \
        DPU_STATS_VAR.nb_bytes_written += sizeof(response);                                                                      \
        mutex_unlock(mutex_responses);                                                                                           \
    } while (0)

#define GET_TIME uint64_t start_matching = perfcounter_get();
#define STORE_MATCHING_TIME                                                                                                      \
    do {                                                                                                                         \
        mutex_lock(mutex_responses);                                                                                             \
        DPU_STATS_VAR.matching_time += perfcounter_get() - start_matching;                                                       \
        DPU_STATS_VAR.nb_match++;                                                                                                \
        mutex_unlock(mutex_responses);                                                                                           \
    } while (0)

#define UPDATE_BYTES_READ                                                                                                        \
    do {                                                                                                                         \
        for (uint32_t each_word = 0; each_word < DPU_REQUEST_VAR.nr_words; each_word++) {                                        \
            DPU_STATS_VAR.nb_bytes_read += get_bytes_read(each_word, each_segment);                                              \
        }                                                                                                                        \
    } while (0)

#else

#define UPDATE_BYTES_WRITTEN
#define GET_TIME
#define STORE_MATCHING_TIME
#define UPDATE_BYTES_READ

#endif

static void can_perform_pos_matching_for_did(did_matcher_t *matchers, unsigned int nr_words, uint32_t did, uint32_t req_id)
{
    start_pos_matching(matchers, nr_words);

    if (!matchers_has_next_pos(matchers, nr_words))
        goto end;

    while (true) {
        uint32_t max_pos, index;

        get_max_pos_and_index(matchers, nr_words, &index, &max_pos);

        switch (seek_pos(matchers, nr_words, max_pos, index)) {
        case POSITIONS_FOUND: {
            uint32_t response_id;
            mutex_lock(mutex_responses);
	    DPU_STATS_VAR.total_results++;
            response_id = DPU_STATS_VAR.nb_results[req_id]++;
            mutex_unlock(mutex_responses);
            if (response_id < MAX_RESPONSES) {
                __dma_aligned response_t response = { .did = did, .pos = max_pos - index };
                mram_write(&response, &DPU_RESPONSES_VAR[req_id][response_id], sizeof(response));
                UPDATE_BYTES_WRITTEN;
            }
            goto end;
        }
        case POSITIONS_NOT_FOUND:
            break;
        case END_OF_POSITIONS:
            goto end;
        }
    }
end:
    stop_pos_matching(matchers, nr_words);
}

static void can_perform_did_and_pos_matching(did_matcher_t *matchers, uint32_t nr_words, uint32_t req_id)
{
    while (true) {
        // This is either the initial loop, or we come back from a
        // set of matching DIDs. Whatever the case is, need to
        // warm up the iterator again by fetching next DIDs.
        if (!matchers_has_next_did(matchers, nr_words))
            return;

        seek_did_t did_status;
        do {
            uint32_t did = get_max_did(matchers, nr_words);
            did_status = seek_did(matchers, nr_words, did);
            switch (did_status) {
            case END_OF_INDEX_TABLE:
                return;
            case DID_FOUND: {
                PRINT("|%u FOUND MATCHING DID::: %d - Checking positions...\n", me(), did);
                GET_TIME;
                can_perform_pos_matching_for_did(matchers, nr_words, did, req_id);
                STORE_MATCHING_TIME;
            } break;
            case DID_NOT_FOUND:
                break;
            }
        } while (did_status == DID_NOT_FOUND);
    }
}

void merge(tuple_t *a, int left, int mid, int right, tuple_t *tmp) {
	int i = left;
	int j = mid;
	int k = left;

	while (i < mid && j < right) {
		if (a[i].key < a[j].key)
			tmp[k++] = a[i++];
		else
			tmp[k++] = a[j++];
	}

	while (i < mid)
		tmp[k++] = a[i++];

	while (j < right)
		tmp[k++] = a[j++];

//	memcpy(a + left, tmp + left, sizeof(tuple_t) * (right - left));
}

int merge_join(tuple_t *tuplesR, tuple_t *tuplesS, int numR, int numS, void *output) {
	int i = 0, j = 0, matches = 0;

	while (i < numR && j < numS) {
		if (tuplesR[i].key < tuplesS[j].key)
			i++;
		else if (tuplesR[i].key > tuplesS[j].key)
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

    for (uint32_t req_id = 0; req_id < MAX_REQUEST_NUM; req_id++) {
    	for (uint32_t each_segment = me(); each_segment < NR_SEGMENTS_PER_MRAM; each_segment += NR_TASKLETS) {
            did_matcher_t *matchers = setup_matchers(DPU_REQUEST_VAR[req_id].nr_words, DPU_REQUEST_VAR[req_id].args, each_segment, req_id);
            can_perform_did_and_pos_matching(matchers, DPU_REQUEST_VAR[req_id].nr_words, req_id);
            UPDATE_BYTES_READ;
    	}
    }

    DPU_STATS_VAR.exec_time = perfcounter_get();

    return 0;
}
