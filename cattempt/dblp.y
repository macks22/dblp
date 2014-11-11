%{
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "logging.h"

extern int verbose;
extern int linect;

%}

%token TITLE_T AUTHOR_T YEAR_T VENUE_T ID_T REF_T ABSTRACT_T
%token BREAK_T EMPTY_REF EMPTY_ABSTRACT

%union
{
    char * str;
}

%type <str> TITLE_T AUTHOR_T YEAR_T VENUE_T ID_T REF_T ABSTRACT_T

%%

paper       :   title authors year venue id refs_list abstract break
                {}
            ;

title       :   TITLE_T
                {printf("title: %s\n", $1); }
            ;

authors     :   AUTHOR_T ',' authors
                {printf("author: %s\n", $1); }
            |   AUTHOR_T
                {printf("author: %s\n", $1); }
            ;

year        :   YEAR_T
                {printf("year: %s\n", $1); }
            ;

venue       :   VENUE_T
                {printf("venue: %s\n", $1); }
            ;

id          :   ID_T
                {printf("id: %s\n", $1); }
            ;

refs_list   :   refs_list '\n' REF_T
                {printf("reference: %s\n", $3); }
            |   REF_T
                {printf("reference: %s\n", $1); }
            |   EMPTY_REF
                {printf("empty reference\n"); }
            ;

abstract    :   ABSTRACT_T
                {printf("abstract: %s\n", $1); }
            |   EMPTY_ABSTRACT
                {printf("empty abstract\n"); }
            ;

break       :   BREAK_T
                {/* process the complete record */}

%%

main (int argc, char **argv) {
    if (argc == 2) {
        if (strcmp(argv[1], "-v") == 0) {
            verbose = LOGGING_LEVEL_INFO;
            log_info("continuing with verbose output\n");
        } else if (strcmp(argv[1], "-vv") == 0) {
            verbose = LOGGING_LEVEL_DEBUG;
            log_info("continuing with debug level verbose output\n");
        } else {
            verbose = 0;
        }
    }
    yyparse();
    return 0;
}

yyerror(char *s) {
   printf("Line %d: %s\n",linect, s);
}
