dblp:	lex.yy.c dblp.tab.c
	gcc -g lex.yy.c dblp.tab.c -o dblp -ly -lfl

lexer: flex lex.yy.c
	gcc -g lex.yy.c -ll -o lexer

flex:
	flex dblp.l

lex.yy.c:	dblp.l
	flex dblp.l

dblp.tab.c:	dblp.y
	bison -vd dblp.y

clean:	
	rm -f lex.yy.c dblp.tab.* uncool.output

realclean:
	rm -f lex.yy.c dblp.tab.* uncool.output uncool
