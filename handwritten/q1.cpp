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

#include <string>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <assert.h>
#include <omp.h>
#include <sys/time.h>
#include <string.h>

#include "utils.h"

#define NUM_PARALLEL_THREADS   48

using namespace std;

// Number of rows in the lineitem table
size_t num_lineitems;

// Scale factor
int SF;

struct Q1Entry {
  int sum_qty;
  double sum_base_price;
  double sum_disc_price;
  double sum_charge;
  double sum_discount;
  int count;  // The average values can be computed from the fields here
};

struct PackedLineitem {
  int returnflag;
  int linestatus;
  int quantity;
  int shipdate;
  double extendedprice;
  double discount;
  double tax;
};

struct Buckets {
  struct Q1Entry entries[3][2];
};

void q1_worker(Lineitem *lineitems, Buckets *b, int tid) {

  memset(b, 0, sizeof(Buckets));

  size_t start = (num_lineitems / NUM_PARALLEL_THREADS) * tid;
  size_t end = start + (num_lineitems / NUM_PARALLEL_THREADS);
  if (end > num_lineitems) {
    end = num_lineitems;
  }

  if (tid == NUM_PARALLEL_THREADS - 1) {
    end = num_lineitems;
  }

  for (size_t i = start; i < end; i++) {
    if (lineitems->shipdate[i] <= 19981201 - 90) {
      struct Q1Entry *entry = &b->entries[lineitems->returnflag[i]][lineitems->linestatus[i]];
      entry->sum_qty += lineitems->quantity[i];
      entry->sum_base_price += lineitems->extendedprice[i];
      entry->sum_disc_price += lineitems->extendedprice[i] * (1 - lineitems->discount[i]);
      entry->sum_charge += lineitems->extendedprice[i] * (1 - lineitems->discount[i]) * (1 + lineitems->tax[i]);
      entry->sum_discount += lineitems->discount[i];
      entry->count++;
    }
  }
}

void q1_worker_packed(PackedLineitem *lineitems, Buckets *b, int tid) {

  memset(b, 0, sizeof(Buckets));

  size_t start = (num_lineitems / NUM_PARALLEL_THREADS) * tid;
  size_t end = start + (num_lineitems / NUM_PARALLEL_THREADS);
  if (end > num_lineitems) {
    end = num_lineitems;
  }

  if (tid == NUM_PARALLEL_THREADS - 1) {
    end = num_lineitems;
  }

  for (size_t i = start; i < end; i++) {
    if (lineitems[i].shipdate <= 19981201 - 90) {
      struct Q1Entry *entry = &b->entries[lineitems[i].returnflag][lineitems[i].linestatus];
      entry->sum_qty += lineitems[i].quantity;
      entry->sum_base_price += lineitems[i].extendedprice;
      entry->sum_disc_price += lineitems[i].extendedprice * (1 - lineitems[i].discount);
      entry->sum_charge += lineitems[i].extendedprice * (1 - lineitems[i].discount) * (1 + lineitems[i].tax);
      entry->sum_discount += lineitems[i].discount;
      entry->count++;
    }
  }
}

void run_query(Lineitem *lineitems) {
  struct timeval before, after, diff;

  Buckets final;
  memset(&final, 0, sizeof(final));

  gettimeofday(&before, 0);

#pragma omp parallel for
  for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {


    Buckets b;
    q1_worker(lineitems, &b, i);

#pragma omp critical(merge)
    {
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 2; j++) {
        final.entries[i][j].sum_qty += b.entries[i][j].sum_qty;
        final.entries[i][j].sum_base_price += b.entries[i][j].sum_base_price;
        final.entries[i][j].sum_disc_price += b.entries[i][j].sum_disc_price;
        final.entries[i][j].sum_charge += b.entries[i][j].sum_charge;
        final.entries[i][j].sum_discount += b.entries[i][j].sum_discount;
        final.entries[i][j].count += b.entries[i][j].count;
      }
    }
    }

  }

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  printf("sum_qty | sum_base_price | sum_disc_price | sum_charge | "
      "avg_qty | avg_price | avg_disc | count_order\n");
  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 2; j++) {
      if (final.entries[i][j].count != 0.0) {
        printf("%d | %f | %f |%f | %d | %f |%f | %d\n",
            final.entries[i][j].sum_qty,
            final.entries[i][j].sum_base_price,
            final.entries[i][j].sum_disc_price,
            final.entries[i][j].sum_charge,
            final.entries[i][j].sum_qty / final.entries[i][j].count,
            final.entries[i][j].sum_base_price / final.entries[i][j].count,
            final.entries[i][j].sum_discount / final.entries[i][j].count,
            final.entries[i][j].count);
      }
    }
  }

  printf("Q1 Complete: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
}

void run_query_packed(PackedLineitem *lineitems) {
  struct timeval before, after, diff;

  Buckets final;
  memset(&final, 0, sizeof(final));

  gettimeofday(&before, 0);

#pragma omp parallel for
  for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {

    struct timeval before, after, diff;
    gettimeofday(&before, 0);

    Buckets b;
    q1_worker_packed(lineitems, &b, i);

#pragma omp critical(merge)
    {
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 2; j++) {
        final.entries[i][j].sum_qty += b.entries[i][j].sum_qty;
        final.entries[i][j].sum_base_price += b.entries[i][j].sum_base_price;
        final.entries[i][j].sum_disc_price += b.entries[i][j].sum_disc_price;
        final.entries[i][j].sum_charge += b.entries[i][j].sum_charge;
        final.entries[i][j].sum_discount += b.entries[i][j].sum_discount;
        final.entries[i][j].count += b.entries[i][j].count;
      }
    }
    }

    gettimeofday(&after, 0);
    timersub(&after, &before, &diff);
    printf("%d - %ld.%06ld\n", i, (long)diff.tv_sec, (long)diff.tv_usec);

  }

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  printf("sum_qty | sum_base_price | sum_disc_price | sum_charge | "
      "avg_qty | avg_price | avg_disc | count_order\n");
  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 2; j++) {
      if (final.entries[i][j].count != 0.0) {
        printf("%d | %f | %f |%f | %d | %f |%f | %d\n",
            final.entries[i][j].sum_qty,
            final.entries[i][j].sum_base_price,
            final.entries[i][j].sum_disc_price,
            final.entries[i][j].sum_charge,
            final.entries[i][j].sum_qty / final.entries[i][j].count,
            final.entries[i][j].sum_base_price / final.entries[i][j].count,
            final.entries[i][j].sum_discount / final.entries[i][j].count,
            final.entries[i][j].count);
      }
    }
  }

  printf("Q1 Complete: %ld.%06ld\n", (long)diff.tv_sec, (long)diff.tv_usec);
}

void loadData_q1(string data_dir, Lineitem *lineitems) {
  string fpath = data_dir + "/lineitem.tbl";
  FILE *lineitem_tbl = fopen(fpath.c_str(), "r");
  num_lineitems = load_lineitems(lineitems, lineitem_tbl, 0);
  fclose(lineitem_tbl);
}

int main(int argc, char **argv) {
  if(!load_sf(argc, argv, SF)) {
    printf("Run as ./q1 -sf <SF>\n");
    return 0;
  }
  string data_dir = "../tpch/sf" + std::to_string(SF);

  Lineitem *lineitems = new Lineitem(6002000 * SF);
  printf("Loading data from %s...\n", data_dir.c_str());
  loadData_q1(data_dir, lineitems);
  printf("Done loading data ... \n");

  int returnflag;
  int linestatus;
  int quantity;
  double extendedprice;
  double discount;
  double tax;

  PackedLineitem *lineitems_packed = (PackedLineitem *)malloc(sizeof(PackedLineitem) * 6002000 * SF);
  for (int i = 0; i < num_lineitems; i++) {
    lineitems_packed[i].returnflag = lineitems->returnflag[i];
    lineitems_packed[i].linestatus = lineitems->linestatus[i];
    lineitems_packed[i].quantity = lineitems->quantity[i];
    lineitems_packed[i].extendedprice = lineitems->extendedprice[i];
    lineitems_packed[i].discount = lineitems->discount[i];
    lineitems_packed[i].tax = lineitems->tax[i];
  }

  delete lineitems;

  //run_query(lineitems);
  run_query_packed(lineitems_packed);
  return 0;
}
