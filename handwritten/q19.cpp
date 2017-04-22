/**
-- TPC-H Query 19
select
        sum(l_extendedprice* (1 - l_discount)) as revenue
from
        lineitem,
        part
where
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#12'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 1 and l_quantity <= 1 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#23'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 10 and l_quantity <= 10 + 10
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#34'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 20 and l_quantity <= 20 + 10
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
*/
#include <string>
#include <cstdlib>
#include <cstdio>
#include <stdint.h>
#include <assert.h>
#include <sys/time.h>
#include <unordered_set>
#include <map>
#include <unordered_map>
#include <vector>
#include <iostream>

#include "utils.h"

#include "../hashtable/dict.h"

#define R 1 // Repeats of each test

#include <omp.h>

#define NUM_PARALLEL_THREADS 16 

using namespace std;

// Global variables.
// Number of rows in the lineitems table.
int num_lineitems;


// Scale factor.
int SF;

/**
 * Builds a dictionary mapping column values to column index
 * @param column the keys to index
 * @param size the length
 * @return index lookup table
 */
 Dict<int, long>& build_index(int* column, long size){
  Dict<int, long> *d = new Dict<int, long>();
  for(long i = 0; i < size; i++){
    d->put(column[i], i);
  }
  return *d;
}


double execute_query(
        Part *p,
        Lineitem *l,
	Dict<int, long>& pk_index,
	int tid);

int run_parallel(Part *p, Lineitem *l) {
  Dict<int, long>& pk_index = build_index(p->partkey, SF * PARTS_PER_SF);
double revenue = 0.0;
#pragma omp parallel for
	for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
		double result = execute_query(p, l, pk_index, i);
		#pragma omp critical
		{
		revenue += result;
		}
	}
return revenue;
}

/**
 * Executes tpch query 19
 * Note that the parts table is already sorted
 * by partkey. The index of the row for a particular
 * partkey is partkey - 1.
 * @param p The parts table
 * @param l The line items table
 * @return  The revenue
 */
double execute_query(
        Part *p,
        Lineitem *l,
	Dict<int, long>& pk_index,
	int tid) {
  double revenue = 0.0;

  int start = (num_lineitems / NUM_PARALLEL_THREADS) * tid;
  int end = start + (num_lineitems / NUM_PARALLEL_THREADS);
  if (tid == NUM_PARALLEL_THREADS - 1) {
    end = num_lineitems;
  }

  for (int i = start; i < end; i++) {
    int pi = *(pk_index.get(l->partkey[i]));
    int p_brand = p->brand[pi];
    int p_container = p->container[pi];
    int p_size = p->size[pi];
    int l_quantity = l->quantity[i];
    int l_shipmode = l->shipmode[i];
    int l_shipinstruct = l->shipinstruct[i];
    if ((p_brand == 12 &&
         (p_container == 11 || p_container == 16 || p_container == 17 || p_container == 13) &&
         (l_quantity >= 1 && l_quantity <= 11) &&
         (p_size >= 1 && p_size <= 5) &&
         l_shipinstruct == 1 &&
         (l_shipmode == 2 || l_shipmode == 3)
        ) ||
        (p_brand == 23 &&
         (p_container == 24 || p_container == 26 || p_container == 23 || p_container == 27) &&
         (l_quantity >= 10 && l_quantity <= 20) &&
         (p_size >= 1 && p_size <= 10) &&
         l_shipinstruct == 1 &&
         (l_shipmode == 2 || l_shipmode == 3)
        ) ||
        (p_brand == 34 &&
         (p_container == 31 || p_container == 36 || p_container == 37 || p_container == 33) &&
         (l_quantity >= 20 && l_quantity <= 30) &&
         (p_size >= 1 && p_size <= 15) &&
         l_shipinstruct == 1 &&
         (l_shipmode == 2 || l_shipmode == 3)
        )
         ) {
      revenue += l->extendedprice[i] * (1.0 - l->discount[i]);
    }
  }
  return revenue;

}
void load_data_q19(string data_dir, Part* parts, Lineitem* lineitems) {
  string fpath = data_dir + "/part.tbl";
  std::cout << "Loading tables from path : " << fpath << std::endl;
  FILE* parts_tbl = fopen(fpath.c_str(), "r");
  load_parts(parts, parts_tbl, 0);
  fclose(parts_tbl);

  fpath = data_dir + "/lineitem.tbl";
  std::cout << "Loading tables from path : " << fpath << std::endl;
  FILE* lineitem_tbl = fopen(fpath.c_str(), "r");
  num_lineitems = load_lineitems(lineitems, lineitem_tbl, 0);
  fclose(lineitem_tbl);
}

int main(int argc, char **argv) {
  if (!load_sf(argc, argv, SF)) {
    printf("Run as ./19 -sf <SF>\n");
    return 0;
  }

  string data_dir = "../tpch/sf" + std::to_string(SF);
  Part *parts = new Part(PARTS_PER_SF * SF);
  Lineitem *lineitems = new Lineitem(LINE_ITEM_PER_SF * SF);
  load_data_q19(data_dir, parts, lineitems);

  printf("Printing Lineitems First 10 rows:\n");
  printf("orderkey | quantity | extendedprice | discount | shipinstruct | shipmode | \n");
  for (int i = 0; i < 10; i++) {
    printf("%d | %d | %lf | %lf | %d | %d |\n",
           lineitems->orderkey[i],
           lineitems->quantity[i],
           lineitems->extendedprice[i],
           lineitems->discount[i],
           lineitems->shipinstruct[i],
           lineitems->shipmode[i]);
  }

  printf("Printing Parts First 10 rows:\n");
  printf("partkey | brand | size | container |\n");
  for (int i = 0; i < 10; i++) {
    printf("%d | %d | %d | %d |\n",
           parts->partkey[i],
           parts->brand[i],
           parts->size[i],
           parts->container[i]);
  }

  struct timeval before, after, diff;
  double res;
  for (int i = 0; i < 5; i++) {
    gettimeofday(&before, 0);
    res = run_parallel(parts, lineitems);
    gettimeofday(&after, 0);
    timersub(&after, &before, &diff);
    printf("Q19 Complete: %ld.%06ld res=%lf\n", (long) diff.tv_sec, (long) diff.tv_usec, res);
  }
}
