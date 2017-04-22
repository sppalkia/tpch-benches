#include <string>
#include <cstdio>
#include <cstdlib>
#include <sys/time.h>
#include <unordered_map>
#include <vector>
#include <mutex>
#include "dict.h"
#include "capped_dict.h"
#include "synchronized_dict.h"
using namespace std;

#define NUM_TUPLES 1<<29 // 536 million.
#define NUM_THREADS 8
typedef long long i64;

/***********************************
 * Herein are many different implementations of distributed
 * hash tables.
 *
 * single_thread_stl => Single thread agg using STL
 * single_thread_with_probe => Single thread using NVL Dict
 * independent_with_probe => Independent hash table per thread + merge
 * global_table => Uses a global hash table
 *
 * *******************************/

void dict_worker(Dict<int, int>& dict, int* keys, int start, int end) {
  for (int i=start; i<end; i++) {
    dict.put(keys[i], 1);
  }
}

// Much slower than the nvl implementation.
void single_thread_stl(int* keys, int num_tuples) {
  unordered_map<int, int> dict;
  for (int i=0; i<num_tuples; i++) {
    dict.insert(make_pair<int, int>(keys[i], 1));
  }
}

void single_thread_with_probe(int* keys, int num_tuples) {
  Dict<int, int> dict;
  dict_worker(dict, keys, 0, num_tuples);
}

void independent_with_probe(int* keys, int num_tuples) {
  Dict<int, int> dicts[NUM_THREADS];

  int range = num_tuples / NUM_THREADS;

#pragma omp parallel for
  for (int i=0; i<NUM_THREADS; i++) {
    int start = i * (num_tuples / NUM_THREADS);
    int end = (i+1) * (num_tuples / NUM_THREADS);
    dict_worker(dicts[i], keys, start, end);
  }

#if 1
#pragma omp parallel for
  for (int i=0; i<NUM_THREADS; i+=2) {
    dicts[i].combine(dicts[i+1]);
  }

  for (int i=2; i<NUM_THREADS; i+=2) {
    dicts[0].combine(dicts[i]);
  }

  printf("Independent: Result Cardinality %d\n", (int)dicts[0].size());
#endif
}

void sync_dict_worker(SynchronizedDict<int, int>& dict, int* keys, int start, int end) {
  for (int i=start; i<end; i++) {
    dict.put(keys[i], 1);
  }
}

void global_table(int* keys, int num_tuples, int capacity) {
  SynchronizedDict<int, int> dict(capacity);

#pragma omp parallel for
  for (int i=0; i<NUM_THREADS; i++) {
    int start = i * (num_tuples / NUM_THREADS);
    int end = (i+1) * (num_tuples / NUM_THREADS);
    sync_dict_worker(dict, keys, start, end);
  }

  printf("Global: Result Cardinality: %d\n", (int)dict.size());
}

void hybrid_worker(int* keys, int start, int end,
    CappedDict<int, int>& dict, vector<int>* buffers) {
  int mask = NUM_THREADS - 1;
  for (int i=start; i<end; i++) {
    bool success = dict.put(keys[i], 1);
    if (!success) {
      int index = keys[i] & mask;
      buffers[index].push_back(keys[i]);
    }
  }
}

void plat(int* keys, int num_tuples) {
  vector<int> buffers[NUM_THREADS][NUM_THREADS];

  CappedDict<int, int> local_dicts[NUM_THREADS];
  for (int i=0; i<NUM_THREADS; i++)
    local_dicts[i].set_max_capacity(1 << 16);

#pragma omp parallel for
  for (int i=0; i<NUM_THREADS; i++) {
    int start = i * (num_tuples / NUM_THREADS);
    int end = (i+1) * (num_tuples / NUM_THREADS);
    hybrid_worker(keys, start, end, local_dicts[i], buffers[i]);
  }

  Dict<int, int> partitioned_dicts[NUM_THREADS];
#pragma omp parallel for
  for (int i=0; i<NUM_THREADS; i++) {
    for (int j=0; j<NUM_THREADS; j++) {
      vector<int>& vec = buffers[j][i];
      int vec_size = vec.size();
      for (int k=0; k<vec_size; k++) {
        partitioned_dicts[i].put(vec[k], 1);
      }
    }
  }

  int mask = NUM_THREADS - 1;
  for (int i=0; i<NUM_THREADS; i++) {
    for (int j=0; j<local_dicts[i]._capacity; j++) {
      if (local_dicts[i]._entries[j].filled) {
        int key = local_dicts[i]._entries[j].key;
        int value = local_dicts[i]._entries[j].value;
        int bucket = key & mask;
        partitioned_dicts[bucket].put(key, value);
      }
    }
  }

  int result_size = 0;
  for (int i=0; i<NUM_THREADS; i++) {
    result_size += (int) partitioned_dicts[i].size();
  }

  printf("PLAT: Result Cardinality: %d\n", result_size);
}

void local_global_worker(int* keys, int start, int end,
    CappedDict<int, int>& dict, Dict<int, int>& global_dict,
    mutex& mu) {
  int mask = NUM_THREADS - 1;
  for (int i=start; i<end; i++) {
    bool success = dict.put(keys[i], 1);
    if (!success) {
      mu.lock();
      for (int i=0; i<dict._capacity; i++) {
        if (dict._entries[i].filled) {
          global_dict.put(dict._entries[i].key, dict._entries[i].value);
          dict._entries[i].filled = false;
        }
      }
      mu.unlock();
    }
  }

  mu.lock();
  for (int i=0; i<dict._capacity; i++) {
    if (dict._entries[i].filled) {
      global_dict.put(dict._entries[i].key, dict._entries[i].value);
      dict._entries[i].filled = false;
    }
  }
  mu.unlock();
}

void local_global(int* keys, int num_tuples) {
  vector<int> buffers[NUM_THREADS][NUM_THREADS];

  CappedDict<int, int> local_dicts[NUM_THREADS];
  Dict<int, int> global_dict;
  for (int i=0; i<NUM_THREADS; i++)
    local_dicts[i].set_max_capacity(1 << 16);

  mutex global_lock;

#pragma omp parallel for
  for (int i=0; i<NUM_THREADS; i++) {
    int start = i * (num_tuples / NUM_THREADS);
    int end = (i+1) * (num_tuples / NUM_THREADS);
    local_global_worker(keys, start, end, local_dicts[i], global_dict, global_lock);
  }

  int result_size = global_dict.size();
  printf("PLAT: Result Cardinality: %d\n", result_size);
}

int* generate_data(string dist, int num_tuples, int distinct_keys) {
  srand(0);
  int mod_mask = distinct_keys - 1;
  int* keys = new int[num_tuples];
  if (dist == "uniform") {
    for (int i=0; i<num_tuples; i++) {
      keys[i] = rand() & mod_mask;
    }
  } else if (dist == "zipf") {
    // TODO:
    // See http://www.cse.usf.edu/~christen/tools/genzipf.c .
  }

  return keys;
}

int main() {
  // for (int i=14; i<15; i+=2) {
  struct timeval before, after, diff1, diff2, diff3, diff4;

  for (int i=24; i>=2; i-=2) {
    int* keys = generate_data("uniform", NUM_TUPLES, 1 << i);

    struct timeval before, after, diff1, diff2, diff3, diff4;

    gettimeofday(&before, 0);
    // single_thread_with_probe(keys, NUM_TUPLES);
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff1);

    gettimeofday(&before, 0);
    independent_with_probe(keys, NUM_TUPLES);
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff2);

    gettimeofday(&before, 0);
    // global_table(keys, NUM_TUPLES, 1 << (i+1));
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff3);

    gettimeofday(&before, 0);
    plat(keys, NUM_TUPLES);
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff4);

    printf("%d %ld.%06ld %ld.%06ld %ld.%06ld %ld.%06ld\n", i,
        (long) diff1.tv_sec, (long) diff1.tv_usec,
        (long) diff2.tv_sec, (long) diff2.tv_usec,
        (long) diff3.tv_sec, (long) diff3.tv_usec,
        (long) diff4.tv_sec, (long) diff4.tv_usec);
  }
}
