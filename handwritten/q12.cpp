/**
 * TPCH Query 12

EXPLAIN select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('MAIL', 'AIR')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1994-01-01'
	and l_receiptdate < date '1994-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode;
where rownum <= -1;
*/

#include <string>
#include <cstdlib>
#include <cstdio>
#include <sys/time.h>
#include <unordered_set>
#include <map>
#include <unordered_map>
#include <vector>
#include <omp.h>
#include <iostream>

#include "utils.h"
using namespace std;

#define NUM_PARALLEL_THREADS 24

// Global variables.
// Number of rows in the lineitems table.
int num_lineitems;

// Scale factor.
int SF;

void partition_withsync(
    Order* o,
    Lineitem* l,
    int partition,
    int results[2][2]) {

  struct timeval before, after, diff;
  gettimeofday(&before, 0);

  int local_results[2][2] = {{0}};

  int start = (partition * num_lineitems) / NUM_PARALLEL_THREADS;
  int end = ((partition + 1) * num_lineitems) / NUM_PARALLEL_THREADS;
  for (int i=start; i<end; i++) {
    if (l->commitdate[i] >= l->recieptdate[i] ||
        !(l->recieptdate[i] >= 19940101 and l->recieptdate[i] < 19950101) ||
        l->shipdate[i] >= l->commitdate[i]) {
      continue;
    }

    int shipmode = l->shipmode[i];
    if (shipmode == 0 || shipmode == 1) {
      int orderpriority = o->orderpriority[l->orderindex[i]];
      if (orderpriority == 1 || orderpriority == 2) {
        local_results[shipmode][0] += 1;
      } else {
        local_results[shipmode][1] += 1;
      }
    }
  }
#pragma omp critical(merge)
  {
    results[0][0] += local_results[0][0];
    results[0][1] += local_results[0][1];
    results[1][0] += local_results[1][0];
    results[1][1] += local_results[1][1];
  }

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);
  printf("Parititon %d -  %ld.%06ld\n", partition, (long) diff.tv_sec, (long) diff.tv_usec);
}

void partition_nosync(
    Order* o,
    Lineitem* l,
    int partition,
    int results[2][2]) {
  int start = (partition * num_lineitems) / NUM_PARALLEL_THREADS;
	int end = ((partition + 1) * num_lineitems) / NUM_PARALLEL_THREADS;
  int order_index = binary_search(o->orderkey, ORDERS_PER_SF * SF, l->orderkey[start]);
  for (int i=start; i<end; i++) {
    if (l->commitdate[i] >= l->recieptdate[i]) continue;
    if (!(l->recieptdate[i] >= 19940101 and l->recieptdate[i] < 19950101)) continue;
    if (l->shipdate[i] >= l->commitdate[i]) continue;

    int shipmode = l->shipmode[i];
    if (shipmode == 0 || shipmode == 1) {
      int orderkey = l->orderkey[i];
      while (o->orderkey[order_index] != orderkey) order_index++;
      int orderpriority = o->orderpriority[order_index];
      if (orderpriority == 1 || orderpriority == 2) {
        results[shipmode][0] += 1;
      } else {
        results[shipmode][1] += 1;
      }
    }
  }
}


void with_sync(Order* orders, Lineitem* lineitems) {
  struct timeval before, after, diff;
  gettimeofday(&before, 0);
	int result[2][2] = {{0}};

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    partition_withsync(orders, lineitems, i, result);
  }
  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  for (int j=0; j<2; j++) {
    printf("%d: %d | %d\n", j, result[j][0], result[j][1]);
  }
  printf("Q12 withsync: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
}


void without_sync(Order* orders, Lineitem* lineitems) {
  struct timeval before, after, diff;
  gettimeofday(&before, 0);
	int partitioned_results[4][2][2] = {{{0}}};

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    partition_nosync(orders, lineitems, i, partitioned_results[i]);
  }

  int result[2][2] = {{0}};
  for (int i=0; i<4; i++) {
    for (int j=0; j<2; j++) {
      for (int k=0; k<2; k++) {
        result[j][k] += partitioned_results[i][j][k];
      }
    }
  }

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);
  for (int j=0; j<2; j++) {
    printf("%d: %d | %d\n", j, result[j][0], result[j][1]);
  }
  printf("Q12 withoutsync: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
}

void load_data_q12(string data_dir, Order* orders, Lineitem* lineitems) {
  string fpath = data_dir + "/orders.tbl";
  FILE* order_tbl = fopen(fpath.c_str(), "r");
  load_orders(orders, order_tbl, 0, 1, SF);
  fclose(order_tbl);

  fpath = data_dir + "/lineitem.tbl";
  FILE* lineitem_tbl = fopen(fpath.c_str(), "r");
  num_lineitems = load_lineitems(lineitems, lineitem_tbl, 0);
  fclose(lineitem_tbl);
}

int main(int argc, char** argv) {
  if (!load_sf(argc, argv, SF)) {
    printf("Run as ./q12 -sf <SF>\n");
    return 0;
  }

  string data_dir = "../tpch/sf" + std::to_string(SF);
  Order* orders = new Order(ORDERS_PER_SF * SF);
  Lineitem* lineitems = new Lineitem(6002000 * SF);
  load_data_q12(data_dir, orders, lineitems);

  int order_index = 0;
  for (int i = 0; i < num_lineitems; i++) {
    int orderkey = lineitems->orderkey[i];
    while (orders->orderkey[order_index] != orderkey) order_index++;
    lineitems->orderindex[i] = order_index;
  }

  for (int i = 0; i < 5; i++) {
     with_sync(orders, lineitems);
  }
  //without_sync(orders, lineitems);

  delete lineitems;
  delete orders;

  return 0;
}

