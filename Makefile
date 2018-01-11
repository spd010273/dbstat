
CC		= gcc
LIBS	= -lm -lpq
CFLAGS  = -I. -I/usr/pgsql-9.6/include

dbstat: dbstat.o
	$(CC) -o dbstat dbstat.o -I. -L/usr/pgsql-9.6/lib -lm -lpq

EXTENSION = dbstat
DATA 	  = $(wildcard sql/$(EXTENSION)--*.sql)
DOCS 	  = README.md
PG_CONFIG = pg_config
PGXS	 := $(shell ${PG_CONFIG} --pgxs)

include ${PGXS}

.PHONY: clean

clean:
	rm -f *.o
