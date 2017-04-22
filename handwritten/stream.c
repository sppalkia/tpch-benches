
#include <stdio.h>
#include <stdlib.h>

#include <sys/time.h>

#include <bsd/stdlib.h>

#include <immintrin.h>

// A gigabyte
#define SIZE (2 * 1000 * 1000 * 1000)

int main() {

    struct timeval before, after, diff;
    size_t length = SIZE / sizeof(int);
    int *buf = (int *)malloc(SIZE);

    __m256i v_sum = _mm256_set1_epi32(0);
    __m128i v_high, v_low;

    arc4random_buf((void *) &buf[0], SIZE);

    long sum = 0;

    gettimeofday(&before, 0);

    for (int i = 0; i + 8 <= length; i+=8) {
        __m256i v_buf = _mm256_load_si256((const __m256i *)(buf + i));
        v_sum = _mm256_add_epi32(v_buf, v_sum);
    }

    v_sum = _mm256_hadd_epi32(v_sum, _mm256_set1_epi32(0));
    v_high = _mm256_extracti128_si256(v_sum, 1);
    v_low = _mm256_castsi256_si128(v_sum);
    v_sum = _mm256_castsi128_si256(_mm_add_epi32(v_low, v_high));

    sum += _mm256_extract_epi32(v_sum, 0) + _mm256_extract_epi32(v_sum, 1);

    gettimeofday(&after, 0);
    timersub(&after, &before, &diff);
    printf("%ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);

    printf("%ld\n", sum);
}
