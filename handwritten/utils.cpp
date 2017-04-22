#include <cstdlib>
#include <string>
#include <cstdio>

#include "utils.h"

#define BUF_SIZE 4096

bool load_sf(int argc, char** argv, int& SF) {
  for (int i=1; i<argc; i++) {
    if (i+1 != argc) {
      if (strcmp(argv[i], "-sf") == 0) {
        SF = atoi(argv[i+1]);
        return true;
      }
    }
  }

  return false;
}

int binary_search(int* arr, int len, int e) {
  int first = 0;
  int last = len - 1;
  int middle = (first+last)/2;
  while (first <= last) {
    if (arr[middle] < e)
      first = middle + 1;
    else if (arr[middle] == e) {
      return middle;
    }
    else
      last = middle - 1;

    middle = (first + last)/2;
  }
  return -1;
}

int parse_date(const char* d) {
  const char z = '0';
  int year = (d[3] - z) + (d[2] - z)*10 + (d[1]-z)*100 + (d[0]-z)*1000;
  int month = (d[6] - z) + (d[5] - z)*10;
  int day = (d[9] - z) + (d[8] - z)*10;
  return day + month * 100 + year * 10000;
}

void load_orders(Order* orders, FILE* tbl, int partition, int num_parts, int sf) {
  char buf[BUF_SIZE];

  if (!tbl) {
    perror("couldn't open orders file");
  }

  int index = (partition * ORDERS_PER_SF * sf) / num_parts;
  while (fgets(buf, BUF_SIZE, tbl)) {
    char* line = buf;
    char* token;
    int column = 0;
    while (column < 8 && (token = strsep(&line, "|")) != NULL) {
      switch (column) {
        case 0:
          orders->orderkey[index] = atoi(token);
          break;
        case 1:
          orders->custkey[index] = atoi(token);
          break;
        case 4:
          orders->orderdate[index] = parse_date(token);
          break;
        case 5:
          orders->orderpriority[index] = token[0] - '0';
          break;
        case 7:
          orders->shippriority[index] = atoi(token);
          break;
        default:
          break;
      }
      column++;
    }

    index++;
  }
}

int load_lineitems(Lineitem* lineitems, FILE* tbl, int offset) {
  char buf[BUF_SIZE];

  if (!tbl) {
    perror("couldn't open lineitems file");
  }

  int index = offset;
  while (fgets(buf, BUF_SIZE, tbl)) {
    char* line = buf;
    char* token;
    int column = 0;
    int commitdate, shipdate, recieptdate;
    while ((token = strsep(&line, "|")) != NULL) {
      switch (column) {
        case 0:
          lineitems->orderkey[index] = atoi(token);
          break;
        case 1:
          lineitems->partkey[index] = atoi(token);
          break;
        case 4:
          lineitems->quantity[index] = atoi(token);
          break;
        case 5:
          lineitems->extendedprice[index] = atof(token);
          break;
        case 6:
          lineitems->discount[index] = atof(token);
          break;
        case 7:
          lineitems->tax[index] = atof(token);
          break;
        case 8:
          if (token[0] == 'N') lineitems->returnflag[index] = 0;
          else if (token[0] == 'R') lineitems->returnflag[index] = 1;
          else lineitems->returnflag[index] = 2;
          break;
        case 9:
          if (token[0] == 'O') lineitems->linestatus[index] = 0;
          else lineitems->linestatus[index] = 1;
          break;
        case 10:
          lineitems->shipdate[index] = parse_date(token);
          break;
        case 11:
          lineitems->commitdate[index] = parse_date(token);
          break;
        case 12:
          lineitems->recieptdate[index] = parse_date(token);
          break;
        case 13:
          if(strcmp(token, "DELIVER IN PERSON") == 0) lineitems->shipinstruct[index] = 1;
          else if(strcmp(token, "TAKE BACK RETURN") == 0) lineitems->shipinstruct[index] = 2;
          else if(strcmp(token, "COLLECT COD") == 0) lineitems->shipinstruct[index] = 3;
          else if(strcmp(token, "NONE") == 0) lineitems->shipinstruct[index] = 4;
          else lineitems->shipinstruct[index] = 0;
          break;
        case 14:
          if (strcmp(token, "MAIL") == 0) lineitems->shipmode[index] = 1;
          else if (strcmp(token, "AIR") == 0) lineitems->shipmode[index] = 2;
          else if (strcmp(token, "AIR REG") == 0) lineitems->shipmode[index] = 3;
          else lineitems->shipmode[index] = 0;
          break;
        default:
          break;
      }
      column++;
    }
    index++;
  }

  return index;
}

void load_customers(Customer* c, FILE* tbl, int partition, int num_parts, int sf) {
  char buf[BUF_SIZE];

  if (!tbl) {
    perror("couldn't open customers file");
  }

  int index = (partition * CUSTOMERS_PER_SF * sf) / num_parts;
  int count = 0;
  while (fgets(buf, BUF_SIZE, tbl)) {
    char* line = buf;
    char* token;
    int column = 0;
    while ((token = strsep(&line, "|")) != NULL) {
      if (column == 6) {
        if (strcmp(token, "MACHINERY") == 0) { count++; c->mktsegment[index] = 1; }
        else c->mktsegment[index] = 0;
      }
      column++;
    }
    index++;
  }
}

void load_parts(Part* parts, FILE* tbl, int offset) {
  char buf[BUF_SIZE];

  if (!tbl) {
    perror("couldn't open parts file");
  }

  int index = offset;
  while (fgets(buf, BUF_SIZE, tbl)) {
    char* line = buf;
    char* token;
    int column = 0;
    while ((token = strsep(&line, "|")) != NULL) {
      switch (column) {
        case 0:
          parts->partkey[index] = atoi(token);
          break;
        case 3:
          int brand;
          sscanf(token, "Brand#%d", &brand);
          parts->brand[index] = brand;
          break;
        case 4:
          int promostr;
          if (strncmp(token, "PROMO", 5) == 0) {
              promostr = 1;
          } else {
              promostr = 0;
          }
          parts->promo_str[index] = promostr;
        case 5:
          parts->size[index] = atoi(token);
          break;
        case 6:
          char case_type[10];
          char case_size[10];
          int type;
          int size;
          sscanf(token, "%s %s", case_size, case_type);
          if(strcmp(case_type, "CASE") == 0) type = 1;
          else if(strcmp(case_type, "DRUM") == 0) type = 2;
          else if(strcmp(case_type, "PKG") == 0) type = 3;
          else if(strcmp(case_type, "BAG") == 0) type = 4;
          else if(strcmp(case_type, "CAN") == 0) type = 5;
          else if(strcmp(case_type, "BOX") == 0) type = 6;
          else if(strcmp(case_type, "PACK") == 0) type = 7;
          else if(strcmp(case_type, "JAR") == 0) type = 8;
          else type = 0;
          if(strcmp(case_size, "SM") == 0) size = 10;
          else if(strcmp(case_size, "MED") == 0) size = 20;
          else if(strcmp(case_size, "LG") == 0) size = 30;
          else if(strcmp(case_size, "JUMBO") == 0) size = 40;
          else if(strcmp(case_size, "WRAP") == 0) size = 50;
          else size = 0;
          parts->container[index] = type + size;
          break;
        default:
          break;
      }
      column++;
    }
    index++;
  }
}
