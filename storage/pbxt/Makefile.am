INCLUDES = 
SUBDIRS = src test bin

EXTRA_DIST = plug.in CMakeLists.txt
.PHONY: test

test:
	rm -f src/.libs/udf_example.so
	cp $(ENG_MYSQL_SRC)/sql/.libs/udf_example.so src/.libs/
	cd test/mysql-test ; \
	./pbxt-test-run.pl



