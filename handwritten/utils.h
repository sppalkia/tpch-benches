#include <cstring>

#define CUSTOMERS_PER_SF 150000
#define ORDERS_PER_SF 1500000
#define LINE_ITEM_PER_SF 6002000
#define PARTS_PER_SF 200000

typedef enum {
    L_ORDERKEY = 0,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT,
} LineitemKeyNames;

typedef enum {
  O_ORDERKEY = 0,
  O_CUSTKEY,
  O_ORDERSTATUS,
  O_TOTALPRICE,
  O_ORDERDATE,
  O_ORDERPRIORITY,
  O_CLERK,
  O_SHIPPRIORITY,
  O_COMMENT,
} OrderKeyItens;

// Sorted by orderkey.
struct Lineitem {
  int* orderkey;
  int* quantity;
  int* partkey;
  double* extendedprice;
  double* discount;
  double* tax;
  int* commitdate;
  int* shipdate;
  int* recieptdate;
  int* shipinstruct;
  int* shipmode;
  int* returnflag;
  int* linestatus;

  // Index in the order table
  int* orderindex;
  //Index in the part table
  int* partindex;

  Lineitem(int n) {
    orderkey = new int[n];
    quantity = new int[n];
    partkey = new int[n];
    extendedprice = new double[n];
    discount = new double[n];
    tax = new double[n];
    commitdate = new int[n];
    shipdate = new int[n];
    recieptdate = new int[n];
    shipinstruct = new int[n];
    shipmode = new int[n];
    returnflag = new int[n];
    linestatus = new int[n];

    partindex = new int[n];
    orderindex = new int[n];
  }

  ~Lineitem() {
    delete orderkey;
    delete quantity;
    delete partkey;
    delete extendedprice;
    delete discount;
    delete tax;
    delete commitdate;
    delete shipdate;
    delete recieptdate;
    delete shipinstruct;
    delete shipmode;
    delete returnflag;
    delete linestatus;

    delete partindex;
    delete orderindex;
  }
};

// Sorted by orderkey.
struct Order {
  int* orderkey;
  int* custkey;
  int* orderdate;
  int* orderpriority;
  int* shippriority;

  // For Q3
  int* li_start;
  int* li_end;

  Order(int n) {
    orderkey = new int[n];
    custkey = new int[n];
    orderdate = new int[n];
    orderpriority = new int[n];
    shippriority = new int[n];

    li_start = new int[n];
    li_end = new int[n];
  }

  ~Order() {
    delete orderkey;
    delete custkey;
    delete orderdate;
    delete orderpriority;
    delete shippriority;

    delete li_start;
    delete li_end;
  }
};

struct Part {
  int* partkey;
  int* brand;
  int* size;
  int* container;
  int* promo_str;

  Part(int n) {
    partkey = new int[n];
    brand = new int[n];
    size = new int[n];
    container = new int[n];
    promo_str = new int[n];
  }

  ~Part() {
    delete partkey;
    delete brand;
    delete size;
    delete container;
    delete promo_str;
  }
};

// Here index + 1 is the order key.
struct Customer {
  int* mktsegment;

  Customer(int n) {
    mktsegment = new int[n];
  }

  ~Customer() {
    delete mktsegment;
  }
};

/** Sets SF from the command line parameters.
 *
 * @param argc num of arguments
 * @param argv command line arguments
 * @param SF scale factor
 *
 * @return true if successful else false
 */
bool load_sf(int argc, char** argv, int& SF);

/** Uses binary search to return index of element in array.
 *
 * @param arr array
 * @param len length of array
 * @param e element
 *
 * @return integer representing index of element in array. returns
 * -1 if element not found.
 */
int binary_search(int* arr, int len, int e);

/** Parse a date formated as YYYY-MM-DD into an integer, maintaining
 * sort order. Exits on failure.
 *
 * @param date a properly formatted date string.
 *
 * @return an integer representing the date. The integer maintains
 * ordering (i.e. earlier dates have smaller values).
 *
 */
int parse_date(const char* d);

/** Loads orders file.
 * If orders file is partitioned into many parts, each part can be loaded
 * in parallel.
 *
 * @param orders array of uninitialized orders
 * @param tbl file pointer to table
 * @param partition file partition being loaded, used to
 * offset into arders array
 * @param num_parts total number of partitions
 * @param sf scale factor
 *
 */
void load_orders(Order* orders, FILE* tbl, int partition, int num_parts, int sf);

/** Loads customers file.
 * If customers file is partitioned into many parts, each part can be loaded
 * in parallel.
 *
 * @param customers array of uninitialized customers
 * @param tbl file pointer to table
 * @param partition file partition being loaded, used to
 * offset into arders array
 * @param num_parts total number of partitions
 * @param sf scale factor
 *
 */
void load_customers(Customer* customers, FILE* tbl, int partition, int num_parts, int sf);

/** Loads lineitems file.
 * If lineitems file is partitioned into many parts, each part can be loaded
 * in parallel.
 *
 * @param lineitems array of uninitialized customers
 * @param tbl file pointer to table
 * @param offset offset into the lineitems array
 *
 */
int load_lineitems(Lineitem* lineitems, FILE* tbl, int offset);

/** Loads the parts file
 *  If parts file is partitioned into many parts, each part can be loaded
 *  in parallel.
 * @param parts array of unintialized parts
 * @param tbl file pointer to table
 * @param offset offset into the parts array
 */
void load_parts(Part* parts, FILE* tbl, int offset);


