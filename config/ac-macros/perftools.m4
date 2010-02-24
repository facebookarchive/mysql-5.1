dnl Auxiliary macro to check for perftools at given path
AC_DEFUN([GOOGLE_CHECK_PERFTOOLS_DIR], [
save_CPPFLAGS="$CPPFLAGS"
save_LIBS="$LIBS"
CPPFLAGS="$PERFTOOLS_INCLUDES $CPPFLAGS"
LIBS="$LIBS $PERFTOOLS_LIBS -lprofiler -lstdc++"
AC_CACHE_VAL([mysql_cv_perftools],
  [AC_TRY_LINK([#include <profiler.h>],
    [ProfilerDisable(); return 0;],
    [mysql_cv_perftools="yes"
    AC_MSG_RESULT([ok])],
    [mysql_cv_perftools="no"])
  ])
CPPFLAGS="$save_CPPFLAGS"
LIBS="$save_LIBS"
])

dnl GOOGLE_CHECK_PERFTOOLS
dnl ------------------------------------------------------------------------
dnl @synopsis GOOGLE_CHECK_PERFTOOLS
dnl
dnl Provides the following configure options:
dnl --with-perftools-dir=DIR
dnl Possible DIR values are:
dnl - "no" - the macro will disable use of compression functions
dnl - "bundled" - means use perftools bundled along with MySQL sources
dnl - empty, or not specified - the macro will try default system
dnl   library (if present), and in case of error will fall back to 
dnl   bundled perftools
dnl - perftools location prefix - given location prefix, the macro expects
dnl   to find the library headers in $prefix/include/google, and binaries in
dnl   $prefix/lib. If perftools headers or binaries weren't found at $prefix, the
dnl   macro bails out with error.
dnl 
dnl If the library was found, this function #defines HAVE_PERFTOOLS
dnl and configure variables PERFTOOLS_INCLUDES (i.e. -I/path/to/perftools/include),
dnl PERFTOOLS_LIBS (i. e. -L/path/to/perftools/lib -lz) and PERFTOOLS_DEPS which is
dnl used in mysql_config and is always the same as PERFTOOLS_LIBS except to
dnl when we use the bundled perftools. In the latter case PERFTOOLS_LIBS points to the
dnl build dir ($top_builddir/perftools), while mysql_config must point to the
dnl installation dir ($pkglibdir), so PERFTOOLS_DEPS is set to point to
dnl $pkglibdir.

AC_DEFUN([GOOGLE_CHECK_PERFTOOLS], [
AC_MSG_CHECKING([for perftools library])
AC_ARG_WITH([perftools-dir],
            AC_HELP_STRING([--with-perftools-dir=DIR],
                           [Provide MySQL with a custom location of
                           the perftools library. Given DIR, perftools library
                           is assumed to be in $DIR/lib and header files
                           in $DIR/include/google.]),
            [mysql_perftools_dir=${withval}],
            [mysql_perftools_dir=""],
            [perftools_dir=${withval}],
            AC_SUBST([perftools_dir]))

case "$mysql_perftools_dir" in
  "no")
    mysql_cv_perftools="no"
    AC_MSG_RESULT([disabled])
    perftools_dir=""
    AC_SUBST([perftools_dir])
    ;;
  "")
    mysql_cv_perftools="no"
    AC_MSG_RESULT([disabled])
    perftools_dir=""
    AC_SUBST([perftools_dir])
    ;;
  *)
    # Test for perftools using all known library file endings
    if test \( -f "$mysql_perftools_dir/lib/libprofiler.a"  -o \
               -f "$mysql_perftools_dir/lib/libprofiler.so" -o \
               -f "$mysql_perftools_dir/lib/libprofiler.sl" -o \
               -f "$mysql_perftools_dir/lib/libprofiler.dylib" \) \
            -a -f "$mysql_perftools_dir/include/google/tcmalloc.h"; then
      PERFTOOLS_INCLUDES="-I$mysql_perftools_dir/include/google"
      PERFTOOLS_LIBS="-L$mysql_perftools_dir/lib"
      GOOGLE_CHECK_PERFTOOLS_DIR
    fi
    if test "x$mysql_cv_perftools" != "xyes"; then 
      AC_MSG_ERROR([headers or binaries were not found in $mysql_perftools_dir/{include/google,lib}])
    fi
    ;;
esac

AC_ARG_ENABLE(perftools-profiling,
    [  --enable-perftools-profiling
                          Profiling using the perftools library.],
    [ enable_perftools_profiling=yes ] )

AC_ARG_ENABLE(perftools-tcmalloc,
    [  --enable-perftools-tcmalloc
                          Use tcmalloc from the perftools library.],
    [ enable_perftools_tcmalloc=yes ] )

AC_ARG_ENABLE(perftools-stacktrace,
    [  --enable-perftools-stacktrace
                          Use the stacktrace stuff from the perftools library.],
    [ enable_perftools_stacktrace=yes ] )

if test "$mysql_cv_perftools" = "yes"; then
  if test "$enable_perftools_profiling" = "yes"; then
    AC_DEFINE([GOOGLE_PROFILE], [1], [Define to enable perftools support])
    PERFTOOLS_LIBS="$PERFTOOLS_LIBS -lprofiler"
  fi

  if test "$enable_perftools_tcmalloc" = "yes"; then
    AC_DEFINE([GOOGLE_TCMALLOC], [1], [Define to enable perftools tcmalloc support])
    PERFTOOLS_LIBS="$PERFTOOLS_LIBS -ltcmalloc_minimal"
  fi

  if test "$enable_perftools_stacktrace" = "yes"; then
    AC_DEFINE([GOOGLE_STACKTRACE], [1], [Define to enable perftools stacktrace stuff])
    PERFTOOLS_LIBS="$PERFTOOLS_LIBS -lstacktrace"
  fi

  if test "x$PERFTOOLS_DEPS" = "x"; then
    PERFTOOLS_DEPS="$PERFTOOLS_LIBS"
  fi
  AC_SUBST([PERFTOOLS_LIBS])
  AC_SUBST([PERFTOOLS_DEPS])
  AC_SUBST([PERFTOOLS_INCLUDES])
fi
])

dnl ------------------------------------------------------------------------
