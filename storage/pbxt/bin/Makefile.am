# Used to build Makefile.in

INCLUDES =   		$(ENG_MYSQL_INC) -I../src

bin_PROGRAMS =		xtstat

xtstat_SOURCES =	xtstat_xt.cc

xtstat_LDADD =		@ENG_MYSQL_SRC@/libmysql/libmysqlclient.la ../src/libxtutil.a
