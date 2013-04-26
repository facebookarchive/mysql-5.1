EXTRA_DIST =		README

dist-hook:
	mkdir -p \
		$(distdir)/mysql-test/extra/binlog_tests \
		$(distdir)/mysql-test/extra/rpl_tests \
		$(distdir)/mysql-test/include \
		$(distdir)/mysql-test/r \
		$(distdir)/mysql-test/t
	-$(INSTALL_DATA) $(srcdir)/mysql-test/t/*.def $(distdir)/mysql-test/t
	$(INSTALL_DATA) $(srcdir)/mysql-test/t/*.test $(distdir)/mysql-test/t
	$(INSTALL_DATA) $(srcdir)/mysql-test/extra/binlog_tests/*.test $(distdir)/mysql-test/extra/binlog_tests
	$(INSTALL_DATA) $(srcdir)/mysql-test/extra/rpl_tests/*.test $(distdir)/mysql-test/extra/rpl_tests
	$(INSTALL_DATA) $(srcdir)/mysql-test/include/*.inc $(distdir)/mysql-test/include
	$(INSTALL_DATA) $(srcdir)/mysql-test/r/*.result $(distdir)$(testdir)/mysql-test/r
	$(INSTALL_DATA) $(srcdir)/mysql-test/r/*.require $(distdir)$(testdir)/mysql-test/r
