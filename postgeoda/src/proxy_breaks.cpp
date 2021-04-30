/**
 * Author: Xun Li <lixun910@gmail.com>
 *
 * Changes:
 * 2021-1-29 Update to use libgeoda 0.0.6; add pg_local_g();
 * add pg_local_gstar()
 * 2021-4-28 Update functions with new BinWeight() constructor for Window query
 */

#include <vector>
#include <iostream>

#include <libgeoda/GeoDaSet.h>
#include <libgeoda/pg/geoms.h>
#include <libgeoda/pg/utils.h>
#include <libgeoda/gda_data.h>
#include "proxy.h"


double* pg_hinge_aggregate(List *data, List *undefs, bool is_hinge15)
{
    std::cout << "Enter pg_hinge_aggregate()" << std::endl;
    ListCell *l;
    size_t nelems = list_length(data);

    std::vector<double> values(nelems, 0);
    std::vector<bool> value_undefs(nelems, false);
    int i = 0;
    foreach(l, data) {
        double* val = (double*) (lfirst(l));
        values[i++] = val[0];
        lwfree(val);
    }
    foreach(l, undefs) {
        bool* undef = (bool*) (lfirst(l));
        value_undefs[i++] = undef[0];
        lwfree(undef);
    }

    std::cout << "gda_hinge15breaks()" << std::endl;
    std::vector<double> breaks;
    if (is_hinge15) {
        breaks = gda_hinge15breaks(values, value_undefs);
    } else {
        breaks = gda_hinge30breaks(values, value_undefs);
    }
    int n = (int)breaks.size();
    double *result = (double*)palloc(sizeof(double) * n);

    for (int i=0; i< n; ++i) {
        result[i] = breaks[i];
    }

    std::cout << "Exit pg_hinge_aggregate()" << std::endl;
    return result;
}

double* pg_percentile_aggregate(List *data, List *undefs, int* n_breaks)
{
    ListCell *l;
    size_t nelems = list_length(data);

    std::vector<double> values(nelems, 0);
    std::vector<bool> value_undefs(nelems, false);
    int i = 0;
    foreach(l, data) {
        double* val = (double*) (lfirst(l));
        values[i++] = val[0];
        lwfree(val);
    }
    foreach(l, undefs) {
        bool* undef = (bool*) (lfirst(l));
        value_undefs[i++] = undef[0];
        lwfree(undef);
    }

    std::vector<double> breaks = gda_percentilebreaks(values, value_undefs);
    int n = (int)breaks.size();
    double *result = (double*)lwalloc(sizeof(double) * n);

    for (int i=0; i< n; ++i) {
        result[i] = breaks[i];
    }

    *n_breaks  = n;
    return result;
}

double* pg_stddev_aggregate(List *data, List *undefs, int* n_breaks)
{
    ListCell *l;
    size_t nelems = list_length(data);

    std::vector<double> values(nelems, 0);
    std::vector<bool> value_undefs(nelems, false);
    int i = 0;
    foreach(l, data) {
        double* val = (double*) (lfirst(l));
        values[i++] = val[0];
        lwfree(val);
    }
    foreach(l, undefs) {
        bool* undef = (bool*) (lfirst(l));
        value_undefs[i++] = undef[0];
        lwfree(undef);
    }

    std::vector<double> breaks = gda_stddevbreaks(values, value_undefs);
    int n = (int)breaks.size();
    double *result = (double*)lwalloc(sizeof(double) * n);

    for (int i=0; i< n; ++i) {
        result[i] = breaks[i];
    }

    *n_breaks  = n;
    return result;
}

double* pg_quantile_aggregate(List *data, List *undefs, int k)
{
    ListCell *l;
    size_t nelems = list_length(data);

    std::vector<double> values(nelems, 0);
    std::vector<bool> value_undefs(nelems, false);
    int i = 0;
    foreach(l, data) {
        double* val = (double*) (lfirst(l));
        values[i++] = val[0];
        lwfree(val);
    }
    foreach(l, undefs) {
        bool* undef = (bool*) (lfirst(l));
        value_undefs[i++] = undef[0];
        lwfree(undef);
    }

    std::vector<double> breaks = gda_quantilebreaks(k, values, value_undefs);
    int n = (int)breaks.size();
    double *result = (double*)lwalloc(sizeof(double) * n);

    for (int i=0; i< n; ++i) {
        result[i] = breaks[i];
    }

    return result;
}

double* pg_naturalbreaks_aggregate(List *data, List *undefs, int k)
{
    ListCell *l;
    size_t nelems = list_length(data);

    std::vector<double> values(nelems, 0);
    std::vector<bool> value_undefs(nelems, false);
    int i = 0;
    foreach(l, data) {
        double* val = (double*) (lfirst(l));
        values[i++] = val[0];
        lwfree(val);
    }
    foreach(l, undefs) {
        bool* undef = (bool*) (lfirst(l));
        value_undefs[i++] = undef[0];
        lwfree(undef);
    }

    std::vector<double> breaks = gda_naturalbreaks(k, values, value_undefs);
    int n = (int)breaks.size();
    double *result = (double*)lwalloc(sizeof(double) * n);

    for (int i=0; i< n; ++i) {
        result[i] = breaks[i];
    }

    return result;
}