/**
Query 3 TPC-H
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'MACHINERY'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-24'
	and l_shipdate > date '1995-03-24'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;
*/

#include <string>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/time.h>
#include <unordered_set>
#include <map>
#include <unordered_map>
#include <vector>
#include <omp.h>
#include <iostream>
#include <algorithm>
#include "utils.h"
using namespace std;

#define NUM_PARALLEL_THREADS 24

// Global variables.
// Number of rows in the lineitems table.
int num_lineitems;

// Scale factor.
int SF;

struct HashEntry {
  int orderdate;
  int shippriority;
  double revenue;
  bool joined;

  HashEntry(int odate, int shipp) {
    orderdate = odate;
    shippriority = shipp;
    revenue = 0;
    joined = false;
  }
};

void filter_and_hash_customers(
    Customer* c,
    int partition,
    unordered_set<int>* target_customers) {
  int start = (partition * CUSTOMERS_PER_SF * SF) / NUM_PARALLEL_THREADS,
      end = ((partition + 1) * CUSTOMERS_PER_SF * SF) / NUM_PARALLEL_THREADS;
  int count = 0;
  for (int i = start; i < end; i++) {
    if (c->mktsegment[i] == 1) {
#pragma omp critical(customerupdate)
          {
            target_customers->insert(i);
            count++;
          }
    }
  }
}

void join_with_orders(
    Order* o,
    int partition,
    unordered_set<int>* customers,
    unordered_map<int, HashEntry*>* orders_map) {
  int cutoff_date = 19950324;
  int start = (partition * ORDERS_PER_SF * SF)/NUM_PARALLEL_THREADS,
      end = ((partition + 1) * ORDERS_PER_SF * SF)/NUM_PARALLEL_THREADS;
  for (int i = start; i < end; i++) {
    if (o->orderdate[i] < cutoff_date &&
        !(customers->find(o->custkey[i] - 1) == customers->end()))  {
          HashEntry* new_order = new HashEntry(o->orderdate[i], o->shippriority[i]);
#pragma omp critical(orderupdate)
          {
            (*orders_map)[o->orderkey[i]] = new_order;
          }
    }
  }
}

void join_with_lineitems(
    Lineitem* l,
    int partition,
    unordered_map<int, HashEntry*>* orders_map) {
  int cutoff_date = 19950324;
  unordered_map<int, HashEntry*>::iterator it;
  int start = (partition * num_lineitems) / NUM_PARALLEL_THREADS,
      end = ((partition + 1) * num_lineitems) / NUM_PARALLEL_THREADS;
  int count = 0;
  for (int i = start; i < end; i++) {
    if (l->shipdate[i] > cutoff_date) {
      count += 1;
      it = orders_map->find(l->orderkey[i]);
      if (it != orders_map->end()) {
        HashEntry* order = it->second;
        order->revenue += l->extendedprice[i] * (1 - l->discount[i]);
        order->joined = true;
      }
    }
  }
}

void with_sync(Customer* customers, Order* orders, Lineitem* lineitems) {
  struct timeval before, after, diff;
  gettimeofday(&before, 0);
  unordered_set<int>* target_customers = new unordered_set<int>();
  unordered_map<int, HashEntry*>* orders_map = new unordered_map<int, HashEntry*>();

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    filter_and_hash_customers(customers, i, target_customers);
  }

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    join_with_orders(orders, i, target_customers, orders_map);
  }

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    join_with_lineitems(lineitems, i, orders_map);
  }

  int count = 0;
  unordered_map<int, HashEntry*>::iterator result_it;
  for (result_it = orders_map->begin(); result_it != orders_map->end(); result_it++) {
    HashEntry* order = result_it->second;
    if (order->joined) {
      count++;
    }
  }

  gettimeofday(&after, 0);

  printf("Number of orders: %ld\n", orders_map->size());
  printf("Number of customers: %ld\n", target_customers->size());
  delete target_customers;

  printf("Result cardinality: %d\n", count);
  delete orders_map;

  timersub(&after, &before, &diff);
  printf("Q3 With Sync: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
}

void run_partition(
    Customer* c,
    Order* o,
    Lineitem* l,
    int partition,
    unordered_map<int, double>* orders_map) {
  int cutoff_date = 19950324;
  int start = (partition * num_lineitems) / NUM_PARALLEL_THREADS,
      end = ((partition + 1) * num_lineitems) / NUM_PARALLEL_THREADS;
  int order_index = binary_search(o->orderkey, ORDERS_PER_SF * SF, l->orderkey[start]);
  for (int i=start; i<end; i++) {
    if (l->shipdate[i] > cutoff_date) {
      int orderkey = l->orderkey[i];
      while (o->orderkey[order_index] != orderkey) order_index++;
      if (o->orderdate[order_index] < cutoff_date) {
        int custkey = o->custkey[order_index] - 1;
        if (c->mktsegment[custkey] == 1) {
#pragma omp critical(mapupdate)
          {
            (*orders_map)[orderkey] += l->extendedprice[i] * (1 - l->discount[i]);
          }
        }
      }
    }
  }
}

void assuming_sorted(Customer* customers, Order* orders, Lineitem* lineitems) {
  struct timeval before, after, diff;
  gettimeofday(&before, 0);
  unordered_map<int, double>* result = new unordered_map<int, double>();

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    run_partition(customers, orders, lineitems, i, result);
  }

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  int count = 0;
  unordered_map<int, double>::iterator result_it;
  for (result_it = result->begin(); result_it != result->end(); result_it++) {
    count++;
  }

  printf("Result cardinality: %d\n", count);
  printf("Q3 Assuming Sorted: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
  delete result;
}

void run_partition_nosync(
    Customer* c,
    Order* o,
    Lineitem* l,
    int partition,
    double* result) {
  int cutoff_date = 19950324;
  int start = (partition * ORDERS_PER_SF * SF) / NUM_PARALLEL_THREADS,
      end = ((partition + 1) * ORDERS_PER_SF * SF) / NUM_PARALLEL_THREADS;

  // Start point in the lineitem array.
  int li_index;
  if (NUM_PARALLEL_THREADS > 1) {
  li_index = binary_search(l->orderkey, num_lineitems, o->orderkey[start]);
  while(li_index > 0 && l->orderkey[li_index - 1] == l->orderkey[li_index])
    li_index--;
  } else {
    li_index = 0;
  }

  for (int i=start; i<end; i++) {
    if (o->orderdate[i] < cutoff_date) {
      int custkey = o->custkey[i] - 1;
      if (c->mktsegment[custkey] == 1) {
        int orderkey = o->orderkey[i];
        while (l->orderkey[li_index] != orderkey) li_index++;
        while (l->orderkey[li_index] == orderkey) {
          if (l->shipdate[li_index] > cutoff_date) {
            result[i] += l->extendedprice[li_index] * (1 - l->discount[li_index]);
          }
          li_index++;
        }
      }
    }
  }
}

void run_partition_nosync_joined(
    Customer* c,
    Order* o,
    Lineitem* l,
    int partition,
    double* result) {
  int cutoff_date = 19950324;
  int start = (partition * ORDERS_PER_SF * SF) / NUM_PARALLEL_THREADS,
      end = ((partition + 1) * ORDERS_PER_SF * SF) / NUM_PARALLEL_THREADS;

  for (int i=start; i<end; i++) {
    if (o->orderdate[i] < cutoff_date) {
      int custkey = o->custkey[i] - 1;
      if (c->mktsegment[custkey] == 1) {
        int orderkey = o->orderkey[i];
        for (int li_index = o->li_start[i]; li_index < o->li_end[i]; li_index++) {
          if (l->shipdate[li_index] > cutoff_date) {
            result[i] += l->extendedprice[li_index] * (1 - l->discount[li_index]);
          }
        }
      }
    }
  }
}

void assuming_sorted_nosync(Customer* customers, Order* orders, Lineitem* lineitems) {
  struct timeval before, after, diff;
  gettimeofday(&before, 0);
  double* result = new double[ORDERS_PER_SF * SF];
  memset(result, 0, sizeof(double) * ORDERS_PER_SF * SF);

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    run_partition_nosync(customers, orders, lineitems, i, result);
  }

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  int count = 0;
  for (int i=0; i<ORDERS_PER_SF*SF; i++) {
    if (result[i] != 0.0) count++;
  }

  printf("Result cardinality: %d\n", count);
  printf("Q3 Assuming Sorted No Sync: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
  delete result;
}

struct Result {
  int orderkey;
  double revenue;
  int orderdate;
  int shippriority;
};

// Sort desc on revenue, asc on orderdate.
bool operator<(const Result& lhs, const Result& rhs) {
  if (lhs.revenue != rhs.revenue) {
    return lhs.revenue > rhs.revenue;
  } else {
    return lhs.orderdate < rhs.orderdate;
  }
}

// Runs the complete query using assuming_sorted method.
void complete_query(Customer* customers, Order* orders, Lineitem* lineitems) {
  struct timeval before, after, diff;

  gettimeofday(&before, 0);

  double* result = new double[ORDERS_PER_SF * SF];
  memset(result, 0, sizeof(double) * ORDERS_PER_SF * SF);

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    run_partition_nosync(customers, orders, lineitems, i, result);
  }

  int count = 0;
  // TODO: Maybe use vector here ?
  Result* results = new Result[ORDERS_PER_SF*SF];
  for (int i=0; i<ORDERS_PER_SF*SF; i++) {
    if (result[i] != 0.0) {
      Result& res = results[count];
      res.orderkey = orders->orderkey[i];
      res.revenue = result[i];
      res.orderdate = orders->orderdate[i];
      res.shippriority = orders->shippriority[i];
      count++;
    }
  }

  sort(results, results + count);

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  printf("orderkey | revenue | orderdate | shippriority\n");
  for (int i=0; i<10; i++) {
     printf("%d | %.2f | %d | %d\n", results[i].orderkey,
         results[i].revenue, results[i].orderdate, results[i].shippriority);
  }

  printf("Result cardinality: %d\n", count);
  printf("Q3 Complete: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
  delete result;
}

// Runs the complete query using assuming_sorted method with prejoined data.
void complete_query_joined(Customer* customers, Order* orders, Lineitem *lineitems) {
  struct timeval before, after, diff;

  gettimeofday(&before, 0);
  double* result = new double[ORDERS_PER_SF * SF];
  memset(result, 0, sizeof(double) * ORDERS_PER_SF * SF);

#pragma omp parallel for
  for (int i=0; i<NUM_PARALLEL_THREADS; i++) {
    run_partition_nosync_joined(customers, orders, lineitems, i, result);
  }

  int count = 0;
  // TODO: Maybe use vector here ?
  Result* results = new Result[ORDERS_PER_SF*SF];
  for (int i=0; i<ORDERS_PER_SF*SF; i++) {
    if (result[i] != 0.0) {
      Result& res = results[count];
      res.orderkey = orders->orderkey[i];
      res.revenue = result[i];
      res.orderdate = orders->orderdate[i];
      res.shippriority = orders->shippriority[i];
      count++;
    }
  }

  sort(results, results + count);

  gettimeofday(&after, 0);
  timersub(&after, &before, &diff);

  printf("orderkey | revenue | orderdate | shippriority\n");
  for (int i=0; i<10; i++) {
     printf("%d | %.2f | %d | %d\n", results[i].orderkey,
         results[i].revenue, results[i].orderdate, results[i].shippriority);
  }

  printf("Result cardinality: %d\n", count);
  printf("Q3 Complete: %ld.%06ld\n", (long) diff.tv_sec, (long) diff.tv_usec);
  delete result;
}

void loadData_q3(string data_dir, Customer* customers, Order* orders, Lineitem* lineitems) {
  string fpath = data_dir + "/customer.tbl";
  FILE* customer_tbl = fopen(fpath.c_str(), "r");
  load_customers(customers, customer_tbl, 0, 1, SF);
  fclose(customer_tbl);

  fpath = data_dir + "/orders.tbl";
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
    printf("Run as ./q3 -sf <SF>\n");
    return 0;
  }

  string data_dir = "../tpch/sf" + std::to_string(SF);
  Customer* customers = new Customer(CUSTOMERS_PER_SF * SF);
  Order* orders = new Order(ORDERS_PER_SF * SF);
  Lineitem* lineitems = new Lineitem(6002000 * SF);
  printf("Loading data ...\n");
  loadData_q3(data_dir, customers, orders, lineitems);
  printf("Done loading data ...\n");

/*  printf("Comparing different approaches to join\n");*/
  //with_sync(customers, orders, lineitems);
  //assuming_sorted(customers, orders, lineitems);
  /*assuming_sorted_nosync(customers, orders, lineitems);*/

  //complete_query(customers, orders, lineitems);

  int li_index = 0;
  for (int i = 0; i < ORDERS_PER_SF * SF; i++) {
    int orderkey = orders->orderkey[i];
    while (lineitems->orderkey[li_index] != orderkey) li_index++;
    orders->li_start[i] = li_index;
    while (lineitems->orderkey[li_index] == orderkey) li_index++;
    orders->li_end[i] = li_index;
  }

  printf("Running complete query\n");

  for (int i = 0; i < 5; i++) {
    complete_query_joined(customers, orders, lineitems);
  }


  return 0;
}

