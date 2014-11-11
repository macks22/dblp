#include <stdarg.h>

extern int linect;
int verbose;

#define LOGGING_LEVEL_INFO 1
#define LOGGING_LEVEL_DEBUG 2

void
log_info(char *fmt, ...)
{   /* Wrap printf in order to log based on info flag. */
    va_list args;
    if (verbose == LOGGING_LEVEL_INFO || verbose == LOGGING_LEVEL_DEBUG) {
        printf("[LINE %d][INFO ]: ", linect);
        va_start(args, fmt);
        vprintf(fmt, args);
        va_end(args);
    }
}

void
log_debug(char *fmt, ...)
{   /* Wrap printf in order to log based on debug flag. */
    va_list args;
    if (verbose == LOGGING_LEVEL_DEBUG) {
        printf("[LINE %d][DEBUG]: ", linect);
        va_start(args, fmt);
        vprintf(fmt, args);
        va_end(args);
    }
}
