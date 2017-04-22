#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include <omp.h>

#include <immintrin.h>

#include "utilold.h"

#define N (12 * 10 * 1000 * 1000)  // Number of input rows
#define R 1                 // Repeats of each test

#define NUM_PARALLEL_THREADS    4

int l_shipdate[N];
int l_discount[N];
int l_quantity[N];
int l_extendedprice[N];

void q6_print_selectivities(int *l_shipdate,
    int *l_discount,
    int *l_quantity,
    int *l_extendedprice,
    size_t length) {

  int s_date = 0;
  int s_quantity = 0;
  int s_discount = 0;

  int s_total = 0;

  for (size_t i = 0; i < length; i++) {

    short ctr = 0;

    if (l_shipdate[i] >= 19940101 && l_shipdate[i] < 19950101) {
      s_date++;
      ctr++;
    }

    if (l_quantity[i] < 24) {
      s_quantity++;
      ctr++;
    }

    if (l_discount[i] >= 5 && l_discount[i] <= 7) {
      s_discount++;
      ctr++;
    }

    if (ctr == 3) {
      s_total++;
    }
  }

  fprintf(stderr, "quantity: %f, discount:%f, s_date:%f, s_total:%f\n",
      ((double)s_quantity/(double)length),
      ((double)s_discount/(double)length),
      ((double)s_date/(double)length),
      ((double)s_total/(double)length));
  fprintf(stderr, "Total rows matching: %d\n", s_total);

  printf("Match Rate: %f\n", (double)s_total / (double)length);
}

/*
 *
 * Query implementations
 *
 */

// The baseline
int q6_columnar(int *l_shipdate, int *l_discount, int *l_quantity,
    int *l_extendedprice, size_t length) {
  int result = 0;
  for (size_t i = 0; i < length; i++) {
    if (l_shipdate[i] >= 19940101 &&
        l_shipdate[i] < 19950101 &&
        l_discount[i] >= 5 &&
        l_discount[i] <= 7 &&
        l_quantity[i] < 24) {
      result += l_extendedprice[i] * l_discount[i];
    }
  }
  return result;
}

int q6_columnar_reordered_preds(int *l_shipdate,
    int *l_discount,
    int *l_quantity,
    int *l_extendedprice,
    size_t length) {

  int result = 0;

  for (size_t i = 0; i < length; i++) {
    if (l_quantity[i] < 24) {
      if (l_discount[i] >= 5 && l_discount[i] <= 7) {
        if (l_shipdate[i] >= 19940101 && l_shipdate[i] < 19950101) {
          result += l_extendedprice[i] * l_discount[i];
        }
      }
    }
  }

  return result;
}




int q6_columnar_simd_compare_unaligned_loads(int *l_shipdate, int *l_discount,
    int *l_quantity, int *l_extendedprice, size_t length, int tid) {
  size_t i;
  int result = 0;

  // The vectors used for comparison.
  // We add (or subtract) the 1 since we're using a > rather than >= instruction
  const __m256i v_shipdate_lower = _mm256_set1_epi32(19940101 - 1);
  const __m256i v_shipdate_upper = _mm256_set1_epi32(19950101);

  const __m256i v_discount_lower = _mm256_set1_epi32(5 - 1);
  const __m256i v_discount_upper = _mm256_set1_epi32(7 + 1);

  const __m256i v_quantity_upper = _mm256_set1_epi32(24);

  __m256i v_sum = _mm256_set1_epi32(0);
  __m128i v_high, v_low;

  int start = (length / NUM_PARALLEL_THREADS) * tid;
  int end = start + (length / NUM_PARALLEL_THREADS);

  if (tid == NUM_PARALLEL_THREADS - 1) {
    end = length;
  }

  for (i = start; i+8 <= end; i += 8) {

    __m256i v_shipdate, v_discount, v_quantity, v_extendedprice;
    __m256i v_p0;

    v_shipdate = _mm256_lddqu_si256((const __m256i *)(l_shipdate + i));
    v_discount = _mm256_lddqu_si256((const __m256i *)(l_discount + i));
    v_quantity = _mm256_lddqu_si256((const __m256i *)(l_quantity + i));

    // Take the bitwise AND of each comparison to create a bitmask, which will select the
    // rows that pass the predicate.
    v_p0 = _mm256_cmpgt_epi32(v_shipdate, v_shipdate_lower);
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_shipdate_upper, v_shipdate));
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_discount, v_discount_lower));
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_discount_upper, v_discount));
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_quantity_upper, v_quantity));

    // Load the appropriate values from extendedprice. Since this instruction zeroes out
    // the unselected lanes, we don't need to reload discount again
    v_extendedprice = _mm256_maskload_epi32(l_extendedprice + i, v_p0);
    v_sum = _mm256_add_epi32(_mm256_mullo_epi32(v_extendedprice, v_discount), v_sum);
  }

  // Handle the fringe
  for (; i < end; i++) {
    result += ((l_shipdate[i] >= 19940101) &
        (l_shipdate[i] < 19950101) &
        (l_discount[i] >= 5) &
        (l_discount[i] <= 7) &
        (l_quantity[i] < 24)) * l_extendedprice[i] * l_discount[i];
  }

  // We can use three instructions here to collapse the eight values using a sum,
  // into two 32-bit values. Then we extract those two values from the vector and
  // add them into the final result.
  v_sum = _mm256_hadd_epi32(v_sum, _mm256_set1_epi32(0));
  v_high = _mm256_extracti128_si256(v_sum, 1);
  v_low = _mm256_castsi256_si128(v_sum);
  v_sum = _mm256_castsi128_si256(_mm_add_epi32(v_low, v_high));

  result += _mm256_extract_epi32(v_sum, 0) + _mm256_extract_epi32(v_sum, 1);

  return result;
}

int run_parallel(int *l_shipdate, int *l_discount,
    int *l_quantity, int *l_extendedprice, size_t length) {

  int final = 0;

#pragma omp parallel for
  for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
    int r = q6_columnar_simd_compare_unaligned_loads(l_shipdate, l_discount, l_quantity,
        l_extendedprice, length, i);

#pragma omp critical(merge)
    {
      final += r;
    }
  }

  return final;

}

int q6_columnar_simd_compare(int *l_shipdate,
    int *l_discount,
    int *l_quantity,
    int *l_extendedprice,
    size_t length) {

  size_t i;
  int result = 0;

  // The vectors used for comparison.
  // We add (or subtract) the 1 since we're using a > rather than >= instruction
  const __m256i v_shipdate_lower = _mm256_set1_epi32(19940101 - 1);
  const __m256i v_shipdate_upper = _mm256_set1_epi32(19950101);

  const __m256i v_discount_lower = _mm256_set1_epi32(5 - 1);
  const __m256i v_discount_upper = _mm256_set1_epi32(7 + 1);

  const __m256i v_quantity_upper = _mm256_set1_epi32(24);

  __m256i v_sum = _mm256_set1_epi32(0);
  __m128i v_high, v_low;

  for (i = 0; i+8 <= length; i += 8) {

    __m256i v_shipdate, v_discount, v_quantity, v_extendedprice;
    __m256i v_p0, v_result;
    __m128i v_high, v_low;

    // For some reason, dereferencing each element explicitly gives much better performance
    // than using the unaligned load instructions.
    v_shipdate = _mm256_set_epi32(l_shipdate[i+7], l_shipdate[i+6], l_shipdate[i+5],
        l_shipdate[i+4], l_shipdate[i+3], l_shipdate[i+2], l_shipdate[i+1], l_shipdate[i]);

    v_discount = _mm256_set_epi32(l_discount[i+7], l_discount[i+6], l_discount[i+5],
        l_discount[i+4], l_discount[i+3], l_discount[i+2], l_discount[i+1], l_discount[i]);

    v_quantity = _mm256_set_epi32(l_quantity[i+7], l_quantity[i+6], l_quantity[i+5],
        l_quantity[i+4], l_quantity[i+3], l_quantity[i+2], l_quantity[i+1], l_quantity[i]);

    // Take the bitwise AND of each comparison to create a bitmask, which will select the
    // rows that pass the predicate.
    v_p0 = _mm256_cmpgt_epi32(v_shipdate, v_shipdate_lower);
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_shipdate_upper, v_shipdate));
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_discount, v_discount_lower));
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_discount_upper, v_discount));
    v_p0 = _mm256_and_si256(v_p0, _mm256_cmpgt_epi32(v_quantity_upper, v_quantity));

    // Load the appropriate values from extendedprice. Since this instruction zeroes out
    // the unselected lanes, we don't need to reload discount again

    // The maskload seems slightly faster than loading all the extendedprice elements.
    // May because there's so many extra instructions to get v_p0 into the right format?
    v_extendedprice = _mm256_maskload_epi32(l_extendedprice + i, v_p0);
    v_sum = _mm256_add_epi32(_mm256_mullo_epi32(v_extendedprice, v_discount), v_sum);

    /*
       v_extendedprice = _mm256_set_epi32(l_extendedprice[i+7], l_extendedprice[i+6], l_extendedprice[i+5],
       l_extendedprice[i+4], l_extendedprice[i+3], l_extendedprice[i+2], l_extendedprice[i+1], l_extendedprice[i]);

       v_sum = _mm256_add_epi32(_mm256_mullo_epi32(
       _mm256_and_si256(_mm256_set1_epi32(0x1), v_p0),
       _mm256_mullo_epi32(v_extendedprice, v_discount)), v_sum);
       */
  }

  // Handle the fringe
  for (; i < length; i++) {
    if (l_shipdate[i] >= 19940101 &&
        l_shipdate[i] < 19950101 &&
        l_discount[i] >= 5 &&
        l_discount[i] <= 7 &&
        l_quantity[i] < 24) {
      result += l_extendedprice[i] * l_discount[i];
    }
  }

  // We can use three instructions here to collapse the eight values using a sum,
  // into two 32-bit values. Then we extract those two values from the vector and
  // add them into the final result.
  v_sum = _mm256_hadd_epi32(v_sum, _mm256_set1_epi32(0));
  v_high = _mm256_extracti128_si256(v_sum, 1);
  v_low = _mm256_castsi256_si128(v_sum);
  v_sum = _mm256_castsi128_si256(_mm_add_epi32(v_low, v_high));

  result += _mm256_extract_epi32(v_sum, 0) + _mm256_extract_epi32(v_sum, 1);

  return result;
}

int q6_columnar_fewer_branches(int *l_shipdate, int *l_discount,
    int *l_quantity, int *l_extendedprice, size_t length) {
  int result = 0;
  for (size_t i = 0; i < length; i++) {
    if ((l_shipdate[i] >= 19940101) &
        (l_shipdate[i] < 19950101) &
        (l_discount[i] >= 5) &
        (l_discount[i] <= 7) &
        (l_quantity[i]) < 24) {
      result += l_extendedprice[i] * l_discount[i];
    }
  }
  return result;
}

int q6_columnar_no_branches(int *l_shipdate, int *l_discount,
    int *l_quantity, int *l_extendedprice, size_t length) {

  int result = 0;
  for (size_t i = 0; i < length; i++) {
    int passed = 0x1 & ((l_shipdate[i] >= 19940101) &
        (l_shipdate[i] < 19950101) &
        (l_discount[i] >= 5) &
        (l_discount[i] <= 7) &
        (l_quantity[i]) < 24);
    result += (l_extendedprice[i] * l_discount[i] * passed);
  }
  return result;
}

int main(int argc, char **argv) {

  FILE *tbl;
  int count;

  long lines = N;

  fprintf(stderr, "Loading data from tpch-dbgen/lineitem.tbl...");
  fflush(stderr);

  tbl = fopen("../tpch/sf10/lineitem.tbl", "r");
  count = loadData_q6(tbl, l_shipdate, l_discount, l_quantity, l_extendedprice, N, -1);
  assert(count >= 0);
  fclose(tbl);

  fprintf(stderr, "loaded %d lines!\n", count);

  struct timeval before, after, diff;
  long res;

  for (int i = 0;  i < 5; i ++) {
    gettimeofday(&before, 0);
    for (int i = 0; i < R; i++) {
      res = run_parallel(l_shipdate, l_discount,
          l_quantity, l_extendedprice, count);
    }
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff);
    printf("Q6 column: %ld.%06ld res=%ld\n", (long) diff.tv_sec, (long) diff.tv_usec, res);
  }
  return 0;
}

