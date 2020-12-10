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

void merge(tuple_t *a, uint32_t left, uint32_t mid, uint32_t right, tuple_t *tmp) {
    uint32_t i = left;
    uint32_t j = mid;
    uint32_t k = left;

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
}

#if 0 // change algo method

// recursive
void __merge_sort(tuple_t *a, uint32_t left, uint32_t right, tuple_t *tmp) {
    uint32_t mid;
    if (left < right - 1) {
        mid = (right + left) >> 1;
        __merge_sort(a, left, mid, tmp);
        __merge_sort(a, mid, right, tmp);
        merge(a, left, mid, right, tmp);
        memcpy(a + left, tmp + left, sizeof(tuple_t) * (right - left));
    }
}

static inline void merge_sort(tuple_t *a, uint32_t len, tuple_t *tmp) {
    __merge_sort(a, 0, len, tmp);
}

#else

// non-recursive
void merge_sort2(tuple_t *a, uint32_t len, tuple_t *tmp) {
    if (len <= 1)
        return;

    uint32_t toggle = 0;
    tuple_t *src, *dst;
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

#endif

uint32_t merge_join(tuple_t *r, tuple_t *s, uint32_t num_r, uint32_t num_s, void *output) {
    uint32_t i = 0, j = 0, matches = 0;

    while (i < num_r && j < num_s) {
        if (r[i].key < s[j].key)
            i++;
        else if (r[i].key > s[j].key)
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

    clock_t t = clock();
    merge_sort(a, size, tmp);
    merge_sort(b, size, tmp);

    uint32_t matches = merge_join(a, b, size, size, tmp);

    t = clock() - t;
    printf("time: %f ms, matches: %d\n", (float)t * 1000 / CLOCKS_PER_SEC, matches);

    print_tuples(a, size);
    assert(is_tuples_sorted(a, size));
    assert(is_tuples_sorted(b, size));

    return 0;

}
