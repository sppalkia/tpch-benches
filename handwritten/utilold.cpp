#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "utilold.h"

//#include <bsd/stdlib.h>

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
} LineItemKeyNames;

/**
 * Functions for loading the TPC-H database, timing functions, etc.
 */

/** Convert a string into a double, or exit if the parse fails.
 *
 * @param str a string representing a double
 *
 */
double extractd_or_fail(const char *str) {
    char *c;
    double result = strtod(str, &c);
    if (c == str) {
        fprintf(stderr, "[extractd_or_fail]\t couldn't parse string %s\n", str);
        exit(1);
    }

    return result;
}

/** Convert a struct timestamp into a double.
 *
 * @param t the timeval.
 *
 * @return a double. Exits on error.
 */
double timestamp_to_double(struct timeval *t) {

    char *num;
    char *c;

    double ret;

    if (asprintf(&num, "%ld.%06ld\n",
                (long) t->tv_sec, (long) t->tv_usec) < 0) {
        return 0.0;
    }

    ret = strtod(num, &c);
    if (c == num) {
        fprintf(stderr, "[timestamp_to_doubld]\t couldn't parse string %s\n", num);
        exit(1);
    }

    free(num);
    return ret;
}


/** Parse a date formated as YYYY-MM-DD into an integer, maintaining
 * sort order. Exits on failure.
 *
 * @param date a properly formatted date string.
 *
 * @return an integer representing the date. The integer maintains
 * ordering (i.e. earlier dates have smaller values).
 *
 */
int parse_date(const char *date) {

    char *buf, *tofree;

    tofree = buf = strdup(date);

    char *c;
    char *token;
    long value;
    long result[3];

    int i = 0;
    while ((token = strsep((char **)(&buf), "-")) != NULL) {

        value = strtoul(token, &c, 10);
        if (token == c) {
            fprintf(stderr, "[parse_date]\t couldn't parse date %s\n", date);
            exit(1);
        }

        result[i] = value;
        i++;

        if (i > 3)
            break;
    }

    free(tofree);
    return result[2] + result[1] * 100 + result[0] * 10000;
}

int loadData_q1(
        FILE *tbl,
        struct LineItem *lineitem,
        size_t length) {

    int i;
    int ep1, ep2, d1, d2, t1, t2;
    int sd1, sd2, sd3, cd1, cd2, cd3, rd1, rd2, rd3;
    char misc[1000];

    for (i = 0; i < length; i++) {

        struct LineItem *l = lineitem + i;
        int ret = fscanf(tbl, "%d|%d|%d|%d|%d|%d.%d|%d.%d|%d.%d|%c|%c|%d-%d-%d|%d-%d-%d|%d-%d-%d|%[^\n]",
                &l->l_orderkey,
                &l->l_partkey,
                &l->l_suppkey,
                &l->l_linenumber,
                &l->l_quantity,
                &ep1, &ep2,
                &d1, &d2,
                &t1, &t2,
                &l->l_returnflag,
                &l->l_linestatus,
                &sd1, &sd2, &sd3,
                &cd1, &cd2, &cd3,
                &rd1, &rd2, &rd3,
                &misc[0]);

        if (ret == EOF) {
            break;
        }

        l->l_extendedprice = ep1 * 100 + ep2;
        l->l_discount = d1 * 100 + d2;
        l->l_tax = t1 * 100 + t2;
        l->l_shipdate = sd1 * 10000 + sd2 * 100 + sd3;
        l->l_commitdate = cd1 * 10000 + cd2 * 100 + cd3;
        l->l_receiptdate = rd1 * 10000 + rd2 * 100 + rd3;
    }

    return i;
}

int loadData_q6(
        FILE *tbl,
        int *l_shipdate,
        int *l_discount,
        int *l_quantity,
        int *l_extendedprice,
        size_t length,
        double pmatch) {

    const int size = 4096;

    char *buf = (char *)malloc(size);

    int i = 0;
    int ret = -1;

    if (!tbl) {
        perror("couldn't open data file");
        goto exit;
    }

    if (pmatch >= 0.0) {
        arc4random_buf((void *) &l_shipdate[0], sizeof(int) * length);
        arc4random_buf((void *) &l_discount[0], sizeof(int) * length);
        arc4random_buf((void *) &l_quantity[0], sizeof(int) * length);
        arc4random_buf((void *) &l_extendedprice[0], sizeof(int) * length);
    }

    while (fgets(buf, size, tbl)) {

        char *line = buf;
        char *token;
        int column = 0;

        if (i >= length)
            break;

        // If a probability is given, don't load the actual data.
        if (pmatch >= 0) {
            int match = ((rand() % 100) < (int)(pmatch * 100));
            if (match) {
                l_shipdate[i] = 19940801;
                l_quantity[i] = 20;
                l_discount[i] = 6;
            }
            else {
                l_shipdate[i] = 19930101;
            }

            i++;
            continue;
        }

        // For now, just loading up the ones that Q6 actually uses,
        // and only loading the column data.
        while ((token = strsep(&line, "|")) != NULL) {

            double value;


            switch (column) {
                case L_SHIPDATE:
                    l_shipdate[i] = parse_date(token);
                    //l_shipdate[i] = 19940801;
                    break;
                case L_QUANTITY:
                    value = extractd_or_fail(token);
                    l_quantity[i] = (int)value;
                    //l_quantity[i] = 20;
                    break;
                case L_DISCOUNT:
                    value = extractd_or_fail(token);
                    value *= 100;
                    l_discount[i] = (int)value;
                    //l_discount[i] = 6;
                    break;
                case L_EXTENDEDPRICE:
                    value = extractd_or_fail(token);
                    l_extendedprice[i] = (int)value;
                    break;
                default:
                    break;
            }

            column++;
        }

        i++;
    }

    ret = i;

exit:
    return ret;
}
