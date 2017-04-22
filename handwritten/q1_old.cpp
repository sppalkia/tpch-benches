/*

SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <omp.h>
#include <sys/time.h>

#include <immintrin.h>

#include "util.h"

#define N (12 * 10 * 1000 * 1000)  // Number of input rows

#define NUM_PARALLEL_THREADS   8

struct LineItem lineitem[N];

struct Q1Entry {
  int sum_qty;
  int sum_base_price;
  int sum_disc_price;
  int sum_charge;
  int sum_discount;
  int count;  // The average values can be computed from the fields here
};

struct PaddedQ1Entry {
  int32_t sum_qty;
  int32_t sum_base_price;
  int32_t sum_disc_price;
  int32_t sum_charge;
  int32_t sum_discount;
  int32_t count;  // The average values can be computed from the fields here

  // to allow use of 256 bit
  int64_t pad;
};

int q1(struct LineItem *data, size_t length) {

    struct Q1Entry entries[3][2];
    memset(entries, 0, sizeof(entries));

    for (size_t i = 0; i < length; i++) {
        if (data[i].l_shipdate <= 19981201 - 90) {

            struct Q1Entry *entry = &entries[data[i].l_returnflag % 3][data[i].l_linestatus & 1];
            entry->sum_qty += data[i].l_quantity;
            entry->sum_base_price += data[i].l_extendedprice;
            entry->sum_disc_price += data[i].l_extendedprice * (1 - data[i].l_discount);
            entry->sum_charge += data[i].l_extendedprice * (1 - data[i].l_discount) * (1 + data[i].l_tax);
            entry->sum_discount += data[i].l_discount;
            entry->count += 1;
        }
    }

    // The average values can be computed here, but we won't do it since it doesn't take long
    return entries[0][0].count + entries[0][1].count +
        entries[1][0].count + entries[1][1].count +
        entries[2][0].count + entries[2][1].count;
}

int q1_worker(struct LineItem *data,
    int tid,
    size_t length) {

  struct PaddedQ1Entry entries[2][2];
  memset(entries, 0, sizeof(entries));

  size_t start = (length / NUM_PARALLEL_THREADS) * tid;
  size_t end = start + (length / NUM_PARALLEL_THREADS);
  if (end > length) {
    end = length;
  }

  if (tid == NUM_PARALLEL_THREADS - 1) {
    end = length;
  }

  for (size_t i = start; i < end; i++) {
    if (data[i].l_shipdate <= 19981201 - 90) {
      struct PaddedQ1Entry *entry =
        &entries[data[i].l_returnflag % 3][data[i].l_linestatus & 1];

      __m256i v_entry = _mm256_lddqu_si256((const __m256i *)entry);
      __m256i v_rhs = _mm256_set_epi32(0,
        0,
        1,
        data[i].l_discount,
        data[i].l_extendedprice * (1 - data[i].l_discount) * (1 + data[i].l_tax),
        data[i].l_extendedprice * (1 - data[i].l_discount),
        data[i].l_extendedprice,
        data[i].l_quantity);

      v_entry = _mm256_add_epi32(v_entry, v_rhs);
      _mm256_storeu_si256((__m256i *)entry, v_entry);
    }
  }

  // The average values can be computed here, but we won't do it since it doesn't take long
  return entries[0][0].count + entries[0][1].count +
    entries[1][0].count + entries[1][1].count +
    entries[2][0].count + entries[2][1].count;
}

int q1_parallel(struct LineItem *data, size_t length) {

  int result[NUM_PARALLEL_THREADS];

#pragma omp parallel for
  for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
    result[i] = q1_worker(data, i, length);
  }

  int global = 0;
  for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
    global += result[i];
  }

  return global;
}

int q1_simd_interior(struct LineItem *data, size_t length) {
    struct PaddedQ1Entry entries[2][2];
    memset(entries, 0, sizeof(entries));

    for (size_t i = 0; i < length; i++) {
        if (data[i].l_shipdate <= 19981201 - 90) {
            struct PaddedQ1Entry *entry = &entries[data[i].l_returnflag & 1][data[i].l_linestatus & 1];

            __m256i v_entry = _mm256_lddqu_si256((const __m256i *)entry);
            __m256i v_rhs = _mm256_set_epi32(0,
                    0,
                    1,
                    data[i].l_discount,
                    data[i].l_extendedprice * (1 - data[i].l_discount) * (1 + data[i].l_tax),
                    data[i].l_extendedprice * (1 - data[i].l_discount),
                    data[i].l_extendedprice,
                    data[i].l_quantity);

            v_entry = _mm256_add_epi32(v_entry, v_rhs);
            {
            _mm256_storeu_si256((__m256i *)entry, v_entry);
            }
        }
    }

    // The average values can be computed here, but we won't do it since it doesn't take long
    return entries[0][0].count + entries[0][1].count +
        entries[1][0].count + entries[1][1].count;
}

int main(int argc, char **argv) {

    FILE *tbl;
    int count;

    long lines = N;
    char *c, *str;

    struct timeval before, after, diff;
    long res;

    double baseline, other;

    if (argc < 2) {
        fprintf(stderr, "usage: %s <lines>\n", argv[0]);
        fprintf(stderr, "\t pass \"-1\" to use default values.\n");
        exit(1);
    }

    str = argv[1];
    lines = strtoul(str, &c, 10);
    if (c == str) {
        fprintf(stderr, "invalid lines %s\n", str);
        exit(1);
    }
    if (lines > N || lines <= 0)
        lines = N;

    fprintf(stderr, "Loading data from lineitem.tbl...");
    fflush(stderr);

    tbl = fopen("../tpch/lineitem.tbl", "r");
    count = loadData_q1(tbl, lineitem, lines);
    assert(count >= 0);
    fclose(tbl);

    fprintf(stderr, "loaded %d lines!\n", count);

    res = 0;

    printf("Lines: %d\n", count);

    gettimeofday(&before, 0);
    res = q1_parallel(lineitem, count);
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff);
    printf("Q1: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
    printf("%d\n", res);

    return 0;

}
