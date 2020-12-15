// release: gcc -O3 -Wall -o ./merge_sort ./merge_sort.c
// debug  : gcc -g -Wall -o ./merge_sort_debug ./merge_sort.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <stdint.h>
#include <stdbool.h>

typedef uint32_t tuple_key_t;
typedef uint32_t tuple_value_t;

typedef struct  {
    tuple_key_t   key;
    tuple_value_t value;
} tuple_t;

#define CACHE_SIZE       1024
#define TUPLES_PER_CACHE ((uint32_t)CACHE_SIZE / sizeof(tuple_t))
#define INVALID_POS      ((uint32_t)-1)

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
    char    *memory;
} cache_mgr_t;

cache_mgr_t cache[3];

void print_tuples(tuple_t *a, uint32_t size);

uint32_t __log2(uint32_t value) {
    uint32_t x = 0;
    while (value > 1) {
        value >>= 1;
        x++;
    }

    return x;
}

void init_cache(cache_mgr_t *mgr, void *memory, uint32_t memory_size, bool is_rd_cache) {
    assert(memory_size % CACHE_SIZE == 0);

    memset(mgr, 0, sizeof(cache_mgr_t));
    
    mgr->pos_mask = TUPLES_PER_CACHE - 1;
    assert((mgr->pos_mask & TUPLES_PER_CACHE) == 0);

    mgr->pos_shift = __log2(sizeof(tuple_t));
    mgr->memory_size = memory_size;
    mgr->is_rd_cache = is_rd_cache;
    mgr->memory = memory;

    mgr->cache.buffer = malloc(CACHE_SIZE);
    assert(mgr->cache.buffer != NULL);

    mgr->cache.pos = INVALID_POS;
    mgr->cache.begin_pos = INVALID_POS;
    mgr->cache.cnt = 0;

//    printf("pos_mask: 0x%x, member_num: %u, pos_shift: %u, memory_size: %u\n",
  //         mgr->pos_mask, TUPLES_PER_CACHE, mgr->pos_shift, mgr->memory_size);
}

void reset_cache(cache_mgr_t *mgr, void *memory, uint32_t memory_size, bool is_rd_cache) {
    assert(memory_size % CACHE_SIZE == 0);

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
        memcpy(&mgr->memory[mgr->cache.begin_pos << mgr->pos_shift], mgr->cache.buffer, CACHE_SIZE);
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
        memcpy(mgr->cache.buffer, &mgr->memory[begin_pos << mgr->pos_shift], CACHE_SIZE);
    else
        memcpy(&mgr->memory[mgr->cache.begin_pos << mgr->pos_shift], mgr->cache.buffer, CACHE_SIZE);

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
void merge_sort(tuple_t *a, uint32_t len, tuple_t *tmp) {
    if (len <= 1)
        return;

    uint32_t toggle = 0;
    cache_mgr_t *srca, *srcb, *dst;
    srca = &cache[0];
    srcb = &cache[1];
    dst = &cache[2];
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

uint32_t merge_join(cache_mgr_t *r, cache_mgr_t *s, uint32_t num_r, uint32_t num_s, void *output) {
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

#if 1 // change dataset

void init_tuples(tuple_t *a, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        a[i].key = i;
        //a[i].value = i + 1;
    }
}

void shuffle_tuples(tuple_t *a, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        uint32_t j = rand() % size;
        tuple_t tmp = a[i];
        a[i] = a[j];
        a[j] = tmp;
    }
}

void generate_dataset(tuple_t *a, uint32_t size) {
    init_tuples(a, size);
    shuffle_tuples(a, size);
}

bool is_tuples_sorted(tuple_t *a, uint32_t size) {
    for (uint32_t i = 0; i < size - 1; i++) {
        if (a[i].key + 1 != a[i + 1].key) {
            printf("i: %u, key: %u, i+1: %u, key: %u\n", i, a[i].key, i+1, a[i+1].key);
            return false; // not sorted
        }
    }

    return true; // sorted
}

#else

void generate_dataset(tuple_t *a, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        a[i].key = rand() % size;
        //a[i].value = i + 1;
    }
}

bool is_tuples_sorted(tuple_t *a, uint32_t size) {
    for (uint32_t i = 0; i < size - 1; i++) {
        if (a[i].key > a[i + 1].key) {
            printf("i: %u, key: %u, i+1: %u, key: %u\n", i, a[i].key, i+1, a[i+1].key);
            return false; // not sorted
        }
    }

    return true; // sorted
}

#endif

#if 1

void print_tuples(tuple_t *a, uint32_t size) {
    if (size > 16)
        size = 16;
    printf("first %d elements: ", 16);
    for (uint32_t i = 0; i < size; i++) {
        printf("%d ", a[i].key);
    }
    printf("\n");
}

#else

void print_tuples(tuple_t *a, uint32_t size) {
    printf("%p, total %d", a, size);
    for (uint32_t i = 0; i < size; i++) {
        if (i % 16 == 0)
            printf("\n%08x: ", i);
        printf("%08x ", a[i].key);
    }
    printf("\n");
}

#endif

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("usage: %s tuples_size", argv[0]);
        return -1;
    }

    int size = atoi(argv[1]);
    assert(size > 0);

    srand(time(NULL));

    tuple_t *a = malloc(size * sizeof(tuple_t));
    assert(a != NULL);

    tuple_t *b = malloc(size * sizeof(tuple_t));
    assert(b != NULL);

    printf("tuples size: %d, tuples memory: %f MB\n", size, (float)size * sizeof(tuple_t) / 1024 / 1024);

    generate_dataset(a, size);
    generate_dataset(b, size);
    print_tuples(a, size);

    tuple_t *tmp = malloc(size * sizeof(tuple_t));
    assert(tmp != NULL);
    memset(tmp, 0, size * sizeof(tuple_t));

    printf("begin merge sort and merge join\n");

    init_cache(&cache[0], a, size * sizeof(tuple_t), true);
    init_cache(&cache[1], b, size * sizeof(tuple_t), true);
    init_cache(&cache[2], tmp, size * sizeof(tuple_t), false);

    clock_t t = clock();

    merge_sort(a, size, tmp);
    merge_sort(b, size, tmp);

    reset_cache(&cache[0], a, size * sizeof(tuple_t), true);
    reset_cache(&cache[1], b, size * sizeof(tuple_t), true);
    uint32_t matches = merge_join(&cache[0], &cache[1], size, size, tmp);

    t = clock() - t;
    printf("time: %f ms, matches: %u\n", (float)t * 1000 / CLOCKS_PER_SEC, matches);

    print_tuples(a, size);
    assert(is_tuples_sorted(a, size));
    assert(is_tuples_sorted(b, size));

    return 0;

}
