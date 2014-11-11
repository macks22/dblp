%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dblp.tab.h"

int linect = 1;     // keep track of line number

%}

%s AUTHORS
/* used to parse out each individual author from the comma-delimited list */

%s REF
/* used to parse out each individual reference */

%s GOT_REF
/* used if reference is found, in order to distinguish empty references */

%s ABST

%s GOT_ABST

title       #\*.*?$
authors     "#@"
author      [^,\n]*
year        "#t".*?$
venue       "#c".*?$
id          "#index"(.*?)$
refstart    "#%"
ref         [0-9]*
abststart   "#!"
abstract    [^\n\r]*
ws          [ \t]+
break       ^[ \t\r]*\n
eol         [(\r\n)|\n]


%%

{title}             {printf("TITLE: %s\n", yytext+2);
                     yylval.str = strdup(yytext+2);
                     return TITLE_T;}

{authors}           {BEGIN AUTHORS; printf("BEGIN AUTHORS SECTION\n");}
<AUTHORS>{author}   {printf("AUTHOR: %s\n", yytext);
                     yylval.str = strdup(yytext);
                     return AUTHOR_T;}
<AUTHORS>\,         {printf("COMMA\n"); return ','; }
<AUTHORS>\n         {linect++;
                     printf("EOL\n");
                     BEGIN INITIAL; }

{year}              {printf("YEAR: %s\n", yytext+2);
                     yylval.str = strdup(yytext+2);
                     return YEAR_T;}
{venue}             {printf("VENUE: %s\n", yytext+2);
                     yylval.str = strdup(yytext+2);
                     return VENUE_T;}
{id}                {printf("ID: %s\n", yytext+6);
                     yylval.str = strdup(yytext+6);
                     return ID_T;}

{refstart}          {BEGIN REF; }
<REF>{ref}          {printf("REF: %s\n", yytext);
                     yylval.str = strdup(yytext);
                     BEGIN GOT_REF;
                    }
<GOT_REF>\n         {linect++; BEGIN INITIAL; return '\n'; }
<REF>\n             {linect++; BEGIN INITIAL;
                     printf("EMPTY REFERENCE\n");
                     return EMPTY_REF; }

{abststart}         {BEGIN ABST; }
<ABST>{ws}          {BEGIN INITIAL;
                     printf("EMPTY ABSTRACT\n");
                     return EMPTY_ABSTRACT; }
<ABST>{abstract}    {printf("ABSTRACT: %s\n", yytext+2);
                     yylval.str = strdup(yytext);
                     BEGIN INITIAL;
                    }
<ABST>\n            {BEGIN INITIAL;
                     linect++;
                     printf("EMPTY ABSTRACT\n");
                     return EMPTY_ABSTRACT;
                    }

{break}             {linect++; printf("BREAK\n"); return BREAK_T; }
{eol}      	    	{linect++; printf("EOL\n"); }
.		            ;

%%
