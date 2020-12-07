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

//#define STATS_ON
typedef uint32_t key_t;
typedef uint32_t value_t;

typedef struct  {
	key_t   key;
	value_t value;
} tuple_t;

#define MAX_REQUEST_NUM 32

/**
 * @def MAX_REQUESTED_WORDS
 * @brief the maximum number of words that can contribute to a request
 *
 */
#define MAX_REQUESTED_WORDS 5

/**
 * @def MAX_ITEM
 * @brief the maximum number of responses sent by a tasklet (forgets any further response)
 *
 */
#define MAX_RESPONSES  (1024/MAX_REQUEST_NUM)

/**
 * @typedef algo_request
 * @brief Structure of a request issued by the host.
 * @var nr_words how many words are processed by the request
 * @var args list of words processed by the request
 */
typedef struct algo_request {
    uint32_t nr_words;
    uint32_t args[MAX_REQUESTED_WORDS];
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
    uint32_t total_results;
    uint32_t nb_results[MAX_REQUEST_NUM];
#ifdef STATS_ON
    uint32_t nb_match;
    uint32_t nb_bytes_read;
    uint32_t nb_bytes_written;
    uint64_t matching_time;
#endif
} algo_stats_t;
#define DPU_STATS_VAR stat

/**
 * @typedef tasklet_response
 * @brief structure of a response sent by one tasklet
 * @var nr_items how many items in this response
 * @var items a table of nr_items responses
 */
typedef struct response {
    uint32_t did;
    uint32_t pos;
} response_t;
#define DPU_RESPONSES_VAR responses

/**
 * @def NR_SEGMENTS_PER_MRAM
 * @brief The number of segment used to generate the MRAM files
 */
#define NR_SEGMENTS_PER_MRAM (16)

#endif // REQUEST_H
