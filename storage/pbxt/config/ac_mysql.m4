dnl ---------------------------------------------------------------------------
dnl Macro: MYSQL_SRC_TEST
dnl ---------------------------------------------------------------------------
AC_DEFUN([MYSQL_SRC_TEST], [
  AC_MSG_CHECKING(for mysql source code)
  AC_ARG_WITH(mysql,
  [[  --with-mysql=DIR        the target MySQL source tree for building the engine,
                          this option is required]],
  [ENG_MYSQL_SRC="$withval"],
  [ENG_MYSQL_SRC="../.."])
  if test "$ENG_MYSQL_SRC" = "no"; then
    ENG_MYSQL_SRC="../.."
  fi;
  # I do this because ../.. fails when using a
  # symbolic link in the storage directory
  if test "$ENG_MYSQL_SRC" = "../.."; then
    ENG_MYSQL_SRC=`pwd`
    ENG_MYSQL_SRC=`dirname "$ENG_MYSQL_SRC"`
    ENG_MYSQL_SRC=`dirname "$ENG_MYSQL_SRC"`
  fi;
  if test -d "$ENG_MYSQL_SRC/sql"; then
    if test -f "$ENG_MYSQL_SRC/Makefile"; then
      ENG_MYSQL_SRC=`(cd $ENG_MYSQL_SRC && pwd;)`
      AC_DEFINE([ENG_MYSQL_SRC], [1], [Source directory for MySQL])
      ENG_MYSQL_INC="-I$ENG_MYSQL_SRC/sql -I$ENG_MYSQL_SRC/include -I$ENG_MYSQL_SRC/regex -I$ENG_MYSQL_SRC"
      AC_MSG_RESULT(["$ENG_MYSQL_SRC"])
    else
      AC_MSG_ERROR(["no Makefile found in $ENG_MYSQL_SRC"])
    fi
  else
    AC_MSG_ERROR(["no MySQL source found at $ENG_MYSQL_SRC"])
  fi
])

dnl ---------------------------------------------------------------------------
dnl Macro: MYSQL_SRC_TEST
dnl ---------------------------------------------------------------------------

dnl ---------------------------------------------------------------------------
dnl Macro: MYSQL_SRC_CONFIG
dnl ---------------------------------------------------------------------------
AC_DEFUN([MYSQL_SRC_CONFIG], [
  AC_MSG_CHECKING(for mysql configuration settings)
  if test -f $ENG_MYSQL_SRC/Makefile ; then
    pbxt_mysql_op=""
    get_variable_value "exec_prefix"
	if test "x$pbxt_mysql_op" != "x" ; then 
		exec_prefix=$pbxt_mysql_op
	fi	
    get_variable_value "prefix"
	if test "x$pbxt_mysql_op" != "x" ; then 
		prefix=$pbxt_mysql_op
	fi	
    get_variable_value "libdir"
	if test "x$pbxt_mysql_op" != "x" ; then 
		# This is the default. We assume if libdir
		# is set to the default, then it has not been
		# set explicitly
		if test "$libdir" = "\${exec_prefix}/lib" ; then 
			libdir=$pbxt_mysql_op
		fi	
	fi	
    get_variable_value "includedir"
	if test "x$pbxt_mysql_op" != "x" ; then 
		includedir=$pbxt_mysql_op
	fi	
    get_variable_value "CFLAGS"
	if test "x$pbxt_mysql_op" != "x" ; then 
		MYSQL_CFLAGS=$pbxt_mysql_op
	else
		MYSQL_CFLAGS=""
	fi	
    get_variable_value "CXXFLAGS"
	if test "x$pbxt_mysql_op" != "x" ; then 
		MYSQL_CXXFLAGS=$pbxt_mysql_op
	else
		MYSQL_CXXFLAGS=""
	fi	
    get_variable_value "CONF_COMMAND"
	if test "x$pbxt_mysql_op" != "x" ; then 
		MYSQL_DEBUG_LEVEL=`echo $pbxt_mysql_op | sed "s/.*--with-debug=//" | sed "s/'.*//"`
	else
		MYSQL_DEBUG_LEVEL="no"
	fi
	AC_MSG_RESULT(["$prefix"])
  else
    AC_MSG_ERROR(["no config.status found at $ENG_MYSQL_SRC"])
  fi
])

dnl ---------------------------------------------------------------------------
dnl Macro: MYSQL_SRC_CONFIG
dnl ---------------------------------------------------------------------------
