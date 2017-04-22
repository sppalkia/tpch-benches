/**
  SELECT
  100.00 * sum(case
  when p_type like 'PROMO%'
  then l_extendedprice * (1 - l_discount)
  else 0
  end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
  FROM
  lineitem,
  part
  WHERE
  l_partkey = p_partkey
  AND l_shipdate >= date '1495-09-01'
  AND l_shipdate < date '1495-09-01' + interval '1' month;

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

#define NUM_PARALLEL_THREADS 1

using namespace std;

// Global variables.
// Number of rows in the lineitems table.
int num_lineitems;

// Scale factor.
int SF;

struct q_result {
    double sum;
    double dived;
};

/**
 * Builds a dictionary mapping column values to column index
 * @param column the keys to index
 * @param size the length
 * @return index lookup table
 */
Dict<int, long>& build_index(int* column, int *values, long size){
    Dict<int, long> *d = new Dict<int, long>();
    for(long i = 0; i < size; i++){
        d->put(column[i], values[i]);
    }
    return *d;
}


struct q_result execute_query(
        Part *p,
        Lineitem *l,
        Dict<int, long>& pk_index,
        int tid);

double run_parallel(Part *p, Lineitem *l) {
    Dict<int, long>& pk_index = build_index(p->partkey, p->promo_str, SF * PARTS_PER_SF);
    struct q_result promo_revenue;
    promo_revenue.sum = 0;
    promo_revenue.dived = 0;
#pragma omp parallel for
    for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
        struct q_result r = execute_query(p, l, pk_index, i);
#pragma omp critical
        {
            promo_revenue.sum += r.sum;
            promo_revenue.dived += r.dived;
        }
    }
    return promo_revenue.sum / promo_revenue.dived;
}

/**
 * Executes tpch query 14
 * Note that the parts table is already sorted
 * by partkey. The index of the row for a particular
 * partkey is partkey - 1.
 * @param p The parts table
 * @param l The line items table
 * @return  The revenue
 */
struct q_result execute_query( 
        Part *p,
        Lineitem *l,
        Dict<int, long>& pk_index,
        int tid) {

    struct q_result r;
    r.sum = 0;
    r.dived = 0;

    int start = (num_lineitems / NUM_PARALLEL_THREADS) * tid;
    int end = start + (num_lineitems / NUM_PARALLEL_THREADS);
    if (tid == NUM_PARALLEL_THREADS - 1) {
        end = num_lineitems;
    }

    for (int i = start; i < end; i++) {
        int p_type = *(pk_index.get(l->partkey[i]));
        int l_shipdate = l->shipdate[i];
        if (l_shipdate >= 19950901 &&
                l_shipdate < 19951001) {
            double sum = l->extendedprice[i] * (1.0 - l->discount[i]);
            r.dived += sum;
            r.sum += p_type ? sum : 0;
        }
    }
    return r;

}
void load_data_q14(string data_dir, Part* parts, Lineitem* lineitems) {
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
        printf("Run as ./14 -sf <SF>\n");
        return 0;
    }

    string data_dir = "../tpch/sf" + std::to_string(SF);
    Part *parts = new Part(PARTS_PER_SF * SF);
    Lineitem *lineitems = new Lineitem(LINE_ITEM_PER_SF * SF);
    load_data_q14(data_dir, parts, lineitems);

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
                parts->promo_str[i],
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
        printf("Q14 Complete: %ld.%06ld res=%lf\n", (long) diff.tv_sec, (long) diff.tv_usec, res);
    }
}
