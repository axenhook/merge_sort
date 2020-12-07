/*
 * Copyright (c) 2014-2017 - uPmem
 */

/**
 * @fn request.h
 * @brief Description of the requests posted to DPUs
 */

#ifndef REQUEST_H
#define REQUEST_H

#include <stdint.h>

typedef uint32_t key_t;
typedef uint32_t value_t;

typedef struct  {
	key_t   key;
	value_t value;
} tuple_t;

#define MRAM_SIZE (20 << 20) // 20MB
#define TUPLES_NUM (((MRAM_SIZE) / sizeof(tuple_t))) // one tuples
#define TUPLES_NUM_PER_TASKLET ((TUPLES_NUM - 1 + NR_TASKLETS) / NR_TASKLETS)
#define MRAM_SIZE_PER_TASKLET  ((MRAM_SIZE - 1 + NR_TASKLETS) / NR_TASKLETS)


/**
 * @typedef algo_request
 * @brief Structure of a request issued by the host.
 * @var nr_words how many words are processed by the request
 * @var args list of words processed by the request
 */
typedef struct algo_request {
    uint32_t r_num; 
    uint32_t s_num;
} algo_request_t;
#define DPU_REQUEST_VAR request

/**
 * @typedef algo_stat
 * @brief structure of statistics
 * @var exec_time total execution time of the algorithm
 * @var nb_results total number of results found by the algorithm
 */
typedef struct algo_stats {
    uint64_t exec_time;
    uint32_t nb_results[NR_TASKLETS];
} algo_stats_t;
#define DPU_STATS_VAR stat

#endif // REQUEST_H
