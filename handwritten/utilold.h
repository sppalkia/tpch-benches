#ifndef __UTIL_H_
#define __UTIL_H_

/* 
 * The line item table, for reference:
 
// create table lineitem (
// 	l_orderkey		bigint not null, -- references o_orderkey
// 	l_partkey		bigint not null, -- references p_partkey (compound fk to partsupp)
// 	l_suppkey		bigint not null, -- references s_suppkey (compound fk to partsupp)
// 	l_linenumber	integer,
// 	l_quantity		decimal,
// 	l_extendedprice	decimal,
// 	l_discount		decimal,
// 	l_tax			decimal,
// 	l_returnflag	char(1),
// 	l_linestatus	char(1),
// 	l_shipdate		date,
// 	l_commitdate	date,
// 	l_receiptdate	date,
// 	l_shipinstruct	char(25),
// 	l_shipmode		char(10),
// 	l_comment		varchar(44),
// 	primary key (l_orderkey, l_linenumber)
// );

*/

struct LineItem {
  int l_orderkey;
  int l_partkey;
  int l_suppkey;
  int l_linenumber;
  int l_quantity;
  int l_extendedprice;
  int l_discount;
  int l_tax;
  char l_returnflag;
  char l_linestatus;
  int l_shipdate;
  int l_commitdate;
  int l_receiptdate;
  int l_shipinstruct;
  int l_shipmode;
  int l_comment;
  //char l_shipinstruct[26];
  //char l_shipmode[11];
  //char l_comment[45];
};



/** A macro to benchmark a function call, test it's result against some return value
 * for correctness, and report a speedup compared to some baseline If baseline is 0,
 * the speedup check is not performed.
 *
 * @param type the return type of the function call.
 * @param expect the expected return value.
 * @param baseline the baseline measurement to compare against. A value of zero means
 * don't compare against anything. This value should be a double.
 * @param trials an integer, the number of trials to run.
 * @param func the function call expression.
 * @param descrip a description of the test to be printed.
 *
 */
#define BENCHMARK_COMPARE($type, $expect, $baseline, $func, $descrip) do {\
    $type $res;\
    struct timeval $tbefore, $tafter, $tdiff;\
    gettimeofday(&$tbefore, 0);\
    $res = $func;\
    gettimeofday(&$tafter, 0);\
    timersub(&$tafter, &$tbefore, &$tdiff);\
    assert($res == $expect);\
    fprintf(stdout, $descrip ": %ld.%06ld\n", (long)$tdiff.tv_sec, (long)$tdiff.tv_usec);\
    if ($baseline != 0.0) {\
        double $other = timestamp_to_double(&$tdiff);\
    }\
} while (0);
//printf("\tSpeedup: %0.3f times faster\n", ($baseline / $other));\

/** Load TPC-H from the shipdate, discount, quantity, and extendedprice
 * rows. Up to length rows will be populated. Data is loaded from the given
 * file. Used for Q6.
 *
 * @param tbl the file pointing to the lineitem data
 * @param l_shipdate the buffer for shipdates
 * @param l_discount the buffer for discounts
 * @param l_quantity the buffer for quantities
 * @param l_extendedprice the buffer for extendedprices
 * @param length the maximum rows to be written
 * @param pmatch the approximate fraction of rows that should match for Q6.
 * If < 0, loads the actual data. Otherwise, fake data is loaded (i.e. the
 * tbl argument is ignored).
 *
 * @return the number of rows written, or -1 on error.
 */
int loadData_q6(
        FILE *tbl,
        int *l_shipdate,
        int *l_discount,
        int *l_quantity,
        int *l_extendedprice,
        size_t length,
        double pmatch);

/** Load TPC-H data in row format. Used for Q1. 
 *
 * @param tbl the file pointing to the lineitem data
 * @param lineitem the row data buffer
 * @param length the length of the buffer
 * @param pmatch the approximate fraction of rows that should match for Q6.
 * If < 0, loads the actual data. Otherwise, fake data is loaded (i.e. the
 * tbl argument is ignored).
 *
 * @return the number of rows written, or -1 on error.
 */
int loadData_q1(
        FILE *tbl,
        struct LineItem *lineitem,
        size_t length);

/** Convert a struct timestamp into a double.
 *
 * @param t the timeval.
 *
 * @return a double. Exits on error.
 */
double timestamp_to_double(struct timeval *t);

#endif
