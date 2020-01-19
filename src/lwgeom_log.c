#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h> /* for tolower */

#include "lwgeom_log.h"

/* Default reporters */
static void default_noticereporter(const char *fmt, va_list ap);
static void default_errorreporter(const char *fmt, va_list ap);
lwreporter lwnotice_var = default_noticereporter;
lwreporter lwerror_var = default_errorreporter;

void
lwnotice(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);

    /* Call the supplied function */
    (*lwnotice_var)(fmt, ap);

    va_end(ap);
}

void
lwerror(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);

    /* Call the supplied function */
    (*lwerror_var)(fmt, ap);

    va_end(ap);
}

void
lwdebug(int level, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);

    /* Call the supplied function */
    (*lwdebug_var)(level, fmt, ap);

    va_end(ap);
}

