/*
 * Copyright (c) 2014-2017 - uPmem
 */

/**
 * @file main.c
 * @brief executes the algorithm on several DPUs with specific MRAM files
 *
 * This program gets a list of MRAM binary images, containing database fragments, as input.
 * It creates one DPU per MRAM image, loads each image on its assigned DPU and performs a
 * search with each DPU.
 *
 * Arguments are the MRAM binary images to be used.
 */
#define _GNU_SOURCE
#include <dpu.h>
#include <dpu_description.h>
#include <dpu_management.h>

#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "request.h"

#define XSTR(x) #x
#define STR(x) XSTR(x)

DPU_INCBIN(dpu_binary, DPU_BINARY)

#define COLOR_GREEN "\e[00;32m"
#define COLOR_RED "\e[00;31m"
#define COLOR_NONE "\e[0m"

#define MAX(a, b) ((a) > (b) ? (a) : (b))

static inline unsigned long long my_clock(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC_RAW, &t);
    return ((unsigned long long)t.tv_nsec + (unsigned long long)t.tv_sec * 1e9);
}

static void print_dpu(const char *msg, long value)
{
    dpu_description_t desc;
    DPU_ASSERT(dpu_get_profile_description(NULL, &desc));
    double dpu_freq = desc->hw.timings.fck_frequency_in_mhz / desc->hw.timings.clock_division;
    dpu_free_description(desc);
    printf("[DPU]  %s = %.3g ms (%.3g Mcc, %f MHz)\n", msg, 1.0e3 * ((double)value) / (dpu_freq * 1.0e6), value / 1e6, dpu_freq);
}

struct load_and_copy_mram_file_into_dpus_context {
    uint32_t *dpu_offset;
    tuple_t  *par;
};

dpu_error_t load_and_copy_mram_file_into_dpus(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    struct load_and_copy_mram_file_into_dpus_context *ctx = (struct load_and_copy_mram_file_into_dpus_context *)args;
    uint32_t *dpu_offset = ctx->dpu_offset;

    uint32_t nr_dpus;
    DPU_ASSERT(dpu_get_nr_dpus(rank, &nr_dpus));

    struct dpu_set_t dpu;
    unsigned int each_dpu;
    DPU_FOREACH (rank, dpu, each_dpu) {
	uint32_t dpu_id = dpu_offset[rank_id] + each_dpu;
        printf("dpu_id: %u, rank_id: %u, each_dpu: %u\n", dpu_id, rank_id, each_dpu);
        DPU_ASSERT(dpu_prepare_xfer(dpu, &ctx->par[dpu_id * TUPLES_NUM *2]));
    }
    DPU_ASSERT(dpu_push_xfer(rank, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, MRAM_SIZE * 2, DPU_XFER_DEFAULT));

    return DPU_OK;
}

static void print_response_from_dpus(struct dpu_set_t dpu_set, response_t *responses, algo_stats_t *stats)
{
    __attribute__((unused)) struct dpu_set_t dpu;
    unsigned int each_dpu;
    DPU_FOREACH (dpu_set, dpu, each_dpu) {
	uint32_t nb_results = 0;
	for (uint32_t i = 0; i < NR_TASKLETS; i++) {
	    nb_results += stats[each_dpu].nb_results[i];
            printf(">> " COLOR_GREEN "dpu %u tasklet %u matches %u" COLOR_NONE "\n", each_dpu, i, stats[each_dpu].nb_reaults[i]);
	}
        
	printf(">> " COLOR_GREEN "dpu %u matches %u" COLOR_NONE "\n", each_dpu, nb_results);
    }
}

struct get_response_from_dpus_context {
    uint64_t *dpu_slowest;
    double *dpu_average;
    double *rank_average;
    response_t *responses;
    algo_stats_t *stats;
    uint32_t *dpu_offset;
};

dpu_error_t get_response_from_dpus(struct dpu_set_t rank, uint32_t rank_id, void *args)
{
    struct get_response_from_dpus_context *ctx = (struct get_response_from_dpus_context *)args;
    uint32_t *dpu_offset = ctx->dpu_offset;
    algo_stats_t *stats = ctx->stats;
    response_t *responses = ctx->responses;
    uint64_t *slowest = &ctx->dpu_slowest[rank_id];
    double *average = &ctx->dpu_average[rank_id];
    double *rank_average = &ctx->rank_average[rank_id];
    uint64_t slowest_dpu_time = *slowest;
    uint64_t slowest_dpu_in_rank_time = 0;
    double average_dpu_time = *average;
    unsigned int each_dpu;
    struct dpu_set_t dpu;

    DPU_FOREACH (rank, dpu, each_dpu) {
        uint32_t this_dpu = each_dpu + dpu_offset[rank_id];
        average_dpu_time += stats[this_dpu].exec_time;
        slowest_dpu_time = MAX(stats[this_dpu].exec_time, slowest_dpu_time);
        slowest_dpu_in_rank_time = MAX(stats[this_dpu].exec_time, slowest_dpu_in_rank_time);
    }

    *slowest = slowest_dpu_time;
    *average = average_dpu_time;
    *rank_average += slowest_dpu_in_rank_time;

    return DPU_OK;
}

#define DEFAULT_MRAM 1
#define DEFAULT_LOOP 1
#define DEFAULT_MRAM_PATH "."

__attribute__((noreturn)) static void usage(FILE *f, int exit_code, const char *exec_name)
{
    /* clang-format off */
    fprintf(f,
            "\nusage: %s [-p <mram_path>] [-m <number_of_mram>] [-l <number_of_loop>] [-n]\n"
            "\n"
            "\t-p \tthe path to the mram location (default: '" DEFAULT_MRAM_PATH "')\n"
            "\t-m \tthe number of mram to used (default: " STR(DEFAULT_MRAM) ")\n"
            "\t-l \tthe number of loop to run (default: " STR(DEFAULT_LOOP) ")\n"
            "\t-n \tavoid loading the MRAM (to be used with caution)\n",
            exec_name);
    /* clang-format on */
    exit(exit_code);
}

static void verify_path_exists(const char *path)
{
    if (access(path, R_OK)) {
        fprintf(stderr, "path '%s' does not exist or is not readable (errno: %i)\n", path, errno);
        exit(EXIT_FAILURE);
    }
}

static void parse_args(int argc, char **argv, unsigned int *nb_mram, unsigned int *nb_loop, bool *load_mram, char **mram_path)
{
    int opt;
    extern char *optarg;
    while ((opt = getopt(argc, argv, "hm:l:np:")) != -1) {
        switch (opt) {
        case 'p':
            *mram_path = strdup(optarg);
            break;
        case 'm':
            *nb_mram = (unsigned int)atoi(optarg);
            break;
        case 'l':
            *nb_loop = (unsigned int)atoi(optarg);
            break;
        case 'n':
            *load_mram = false;
            break;
        case 'h':
            usage(stdout, EXIT_SUCCESS, argv[0]);
        default:
            usage(stderr, EXIT_FAILURE, argv[0]);
        }
    }
    verify_path_exists(*mram_path);
}

__attribute__((noinline)) void compute_once(
    struct dpu_set_t dpu_set, struct get_response_from_dpus_context *ctx, algo_request_t *request)
{
    DPU_ASSERT(dpu_broadcast_to(dpu_set, STR(DPU_REQUEST_VAR), 0, (void *)request, sizeof(algo_request_t), DPU_XFER_ASYNC));
    DPU_ASSERT(dpu_launch(dpu_set, DPU_ASYNCHRONOUS));
    struct dpu_set_t dpu, rank;
    uint32_t each_dpu, each_rank;
    DPU_RANK_FOREACH (dpu_set, rank, each_rank) {
        DPU_FOREACH (rank, dpu, each_dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &ctx->stats[each_dpu + ctx->dpu_offset[each_rank]]));
        }
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU, STR(DPU_STATS_VAR), 0, sizeof(algo_stats_t), DPU_XFER_ASYNC));
    DPU_ASSERT(dpu_callback(dpu_set, get_response_from_dpus, ctx, DPU_CALLBACK_ASYNC));
}

__attribute__((noinline)) void compute_loop(
    struct dpu_set_t dpu_set, uint32_t nb_loop, struct get_response_from_dpus_context *ctx, algo_request_t *request)
{
    unsigned long long t = my_clock();
    for (unsigned int each_loop = 0; each_loop < nb_loop; each_loop++) {
  //      unsigned long long t = my_clock();
        compute_once(dpu_set, ctx, request);
//	printf("loop: %d, time: %llu ns\n", each_loop, my_clock() - t);
    }
    DPU_ASSERT(dpu_sync(dpu_set));
    t = my_clock() - t;
    printf("nb_loop: %d, time: %llu ns, throughput: %u\n", nb_loop, t, (unsigned int)(((unsigned long long)nb_loop*1e9) / t));
}


void init_tuples(tuple_t *a, int size) {
	for (int i = 0; i < size; i++) {
		a[i].key = i + 1;
		//a[i].value = i + 1;
	}
}

void shuffle_tuples(tuple_t *a, int size) {
	for (int i = 0; i < size; i++) {
		int j = rand() % size;
		tuple_t tmp = a[i];
		a[i] = a[j];
		a[j] = tmp;
	}
}

void generate_dataset1(tuple_t *a, int size) {
	init_tuples(a, size);
	shuffle_tuples(a, size);
}

void partition_tuples(tuple_t *a, int size, tuple_t *par, uint32_t par_num, uint32_t par_off, uint32_t par_size) {
    uint32_t offset[par_num] = {0};
    for (uint32_t i = 0; i < par_num; i++) {
        offset[i] = par_off + i * par_size;
    }

    for (uint32_t i = 0; i < size; i++) {
        uint32_t par_id = a[i].key % par_num;
	assert(offset[par_id] < par_size);
	par[offset[par_id]] = a[i];
	offset[par_id]++;
    }
}

static void allocated_and_compute(struct dpu_set_t dpu_set, uint32_t nr_ranks, algo_request_t *request, uint32_t nb_mram,
    uint32_t nb_loop, char *mram_path, bool load_mram)
{
    // Set dpu_offset
    uint32_t dpu_offset[nr_ranks];
    dpu_offset[0] = 0;

    struct dpu_set_t rank;
    uint32_t each_rank;
    DPU_RANK_FOREACH (dpu_set, rank, each_rank) {
        uint32_t nr_dpus;
        DPU_ASSERT(dpu_get_nr_dpus(rank, &nr_dpus));
        if (each_rank < nr_ranks - 1) {
            dpu_offset[each_rank + 1] = dpu_offset[each_rank] + nr_dpus;
        }
    }

    uint32_t size = nb_mram * TUPLES_NUM * sizeof(tuple_t);
    tuple_t *r = malloc(size);
    assert(r != NULL);

    tuple_t *s = malloc(size);
    assert(s != NULL);

    printf("dpu count: %u, tpules size: %u, tuples memory: %f MB\n", nb_mram, TUPLES_NUM, (float)size / 1024 / 1024);

    generate_dataset1(r, nb_mram * TUPLES_NUM);
    generate_dataset1(s, nb_mram * TUPLES_NUM);

    tuple_t *par = malloc(nb_mram * MRAM_SIZE * 2);
    assert(par != NULL);

    partition_tuples(r, nb_mram * TUPLES_NUM, par, nb_mram, 0, TUPLES_NUM);
    partition_tuples(s, nb_mram * TUPLES_NUM, par, nb_mram, TUPLES_NUM, TUPLES_NUM);

    if (load_mram) {
        printf("Preparing %u MRAMs: loading files\n", nb_mram);
        struct load_and_copy_mram_file_into_dpus_context ctx = { .dpu_offset = dpu_offset, .par = par };
        // Using callback to load each mrams (from disk) in parallel
        DPU_ASSERT(dpu_callback(dpu_set, load_and_copy_mram_file_into_dpus, &ctx, DPU_CALLBACK_DEFAULT));
    } else {
        printf("Using %u MRAMs already loaded\n", nb_mram);
    }

    printf("Initializing buffers\n");
    response_t responses[nb_mram * MAX_RESPONSES];
    algo_stats_t stats[nb_mram];
    uint64_t dpu_slowest[nr_ranks];
    double dpu_average[nr_ranks];
    double rank_average[nr_ranks];
    memset(dpu_slowest, 0, sizeof(dpu_slowest));
    memset(dpu_average, 0, sizeof(dpu_average));
    memset(rank_average, 0, sizeof(rank_average));

    printf("Computing %u loop\n", nb_loop);
    struct get_response_from_dpus_context response_ctx = { .dpu_slowest = dpu_slowest,
        .dpu_average = dpu_average,
        .rank_average = rank_average,
        .responses = responses,
        .stats = stats,
        .dpu_offset = dpu_offset };
    compute_loop(dpu_set, nb_loop, &response_ctx, request);

    double dpu_average_total = 0.0, rank_average_total = 0.0;
    uint64_t dpu_slowest_total = 0ULL;
    DPU_RANK_FOREACH (dpu_set, rank, each_rank) {
        dpu_average_total += dpu_average[each_rank];
        rank_average_total += rank_average[each_rank];
        if (dpu_slowest[each_rank] > dpu_slowest_total)
            dpu_slowest_total = dpu_slowest[each_rank];
    }

    print_response_from_dpus(dpu_set, responses, stats);

    print_dpu("slowest execution time      ", dpu_slowest_total);
    print_dpu("average dpu execution time  ", dpu_average_total / (nb_mram * nb_loop));
    print_dpu("average rank execution time ", rank_average_total / (nr_ranks * nb_loop));
}

/**
 * @brief Main of the Host Application.
 *
 * Expects to get a list of MRAM files to load into the target DPUs.
 */
int main(int argc, char **argv)
{
    struct dpu_set_t dpu_set;
    uint32_t nr_ranks;

    algo_request_t request = {.r_num = TUPLES_NUM, .s_num = TUPLES_NUM};

    unsigned int nb_mram = DEFAULT_MRAM;
    unsigned int nb_loop = DEFAULT_LOOP;
    char *mram_path = DEFAULT_MRAM_PATH;
    bool load_mram = true;
    parse_args(argc, argv, &nb_mram, &nb_loop, &load_mram, &mram_path);

    printf("Allocating DPUs\n");
    DPU_ASSERT(dpu_alloc(nb_mram, "cycleAccurate=true", &dpu_set));
    DPU_ASSERT(dpu_load_from_incbin(dpu_set, &dpu_binary, NULL));
    DPU_ASSERT(dpu_get_nr_ranks(dpu_set, &nr_ranks));

    allocated_and_compute(dpu_set, nr_ranks, &request, nb_mram, nb_loop, mram_path, load_mram);

    DPU_ASSERT(dpu_free(dpu_set));

    return 0;
}
