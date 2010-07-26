#!/bin/bash
#
# mysqld	This shell script takes care of starting and stopping
#		the MySQL subsystem (mysqld).
#
# chkconfig: - 64 36
# description:	MySQL database server.
# processname: mysqld
# config: /etc/my.cnf
# pidfile: /var/run/mysqld/mysqld.pid

# Source function library.
. /etc/rc.d/init.d/functions

# Source networking configuration.
. /etc/sysconfig/network


prog="MySQL"

if [ "`echo $@ | grep -Pc '(\s+|^)--force(\s+|$)'`" == "1" ]; then
  force=1
else
  force=0
fi

# extract value of a MySQL option from config files
# Usage: get_mysql_option SECTION VARNAME DEFAULT
# result is returned in $result
# We use my_print_defaults which prints all options from multiple files,
# with the more specific ones later; hence take the last match.
get_mysql_option(){
	result=`/usr/bin/my_print_defaults "$1" | sed -n "s/^--$2=//p" | tail -n 1`
	if [ -z "$result" ]; then
	    # not found, use default
	    result="$3"
	fi
}

get_mysql_option mysqld datadir "/var/lib/mysql"
datadir="$result"
get_mysql_option mysqld socket "$datadir/mysql.sock"
socketfile="$result"
get_mysql_option mysqld_safe log-error "/var/log/mysqld.log"
errlogfile="$result"
get_mysql_option mysqld_safe pid-file "/var/run/mysqld/mysqld.pid"
mypidfile="$result"
get_mysql_option mysqld innodb_data_home_dir ""
innodb_data_home_dir="$result"

# if the host is fio, make sure the fio card is mounted
function check_fio_mount(){
  fio_in_dmesg=`dmesg | grep 'fioinf ioDrive' | wc -l`

  if [ "$fio_in_dmesg" != "0" ]; then
    has_flashcache=`ls /dev/* | grep -c '/dev/fioa[12]'`

    if [ "$has_flashcache" == '2' ]; then
      #TODO write a new function to detect if flashcache is enabled
      return 0
    fi

    mounted_fio=`mount | grep -c '/dev/fio'`
    if [ "$mounted_fio" == "0" ]; then
      return 1
    fi
  fi

  return 0
}

function check_mysql_procs(){
  if [ "`/usr/bin/pgrep mysqld_safe | wc -l`" == "0" ]; then
    return 0
  else
    return 1
  fi
}

start(){
	touch "$errlogfile"
	chown mysql:mysql "$errlogfile"
	chmod 0640 "$errlogfile"
	[ -x /sbin/restorecon ] && /sbin/restorecon "$errlogfile"

  # sanity check current setup
  if [ $force -ne 1 ]; then
    check_fio_mount
    if [ $? -ne 0 ]; then
      echo 'Fusion IO detected but not mounted (and not flashcache)'
      action $"Starting $prog: " /bin/false
      return 1
    fi
  	if [ ! -f '/etc/my.cnf' ]; then
      echo File not found: /etc/my.cnf
      action $"Starting $prog: " /bin/false
      return 1
    fi
    if [ ! -d "$datadir/mysql" ] ; then
        # we don't need to accidentally init the db here
        echo "MySQL database not found at $datadir/mysql"
        action $"Starting $prog: " /bin/false
        return 1
	  fi
    if [ ! -f "$innodb_data_home_dir/ibdata1" ]; then
      echo File not found: $innodb_data_home_dir/ibdata1
      action $"Starting $prog: " /bin/false
      return 1
    fi
    check_mysql_procs
    if [ $? -ne 0 ]; then
      echo mysqld process detected already running
      action $"Starting $prog: " /bin/false
      return 1
    fi
	fi
  chown mysql:mysql "$datadir"
	chmod 0755 "$datadir"
 
  # Pass all the options determined above, to ensure consistent behavior.
	# In many cases mysqld_safe would arrive at the same conclusions anyway
	# but we need to be sure.
	/usr/bin/mysqld_safe   --datadir="$datadir" --socket="$socketfile" \
		--log-error="$errlogfile" --pid-file="$mypidfile" \
		--user=mysql >/dev/null 2>&1 &
	ret=$?
  sleep 1
	# Spin for a maximum of N seconds waiting for the server to come up.
	# Rather than assuming we know a valid username, accept an "access
	# denied" response as meaning the server is functioning.
	if [ $ret -eq 0 ]; then
	    STARTTIMEOUT=60
	    while [ $STARTTIMEOUT -gt 0 ]; do
		RESPONSE=`/usr/bin/mysqladmin --socket="$socketfile" --user=UNKNOWN_MYSQL_USER ping 2>&1` && break
		echo "$RESPONSE" | grep -q "Access denied for user" && break
    
    sleep 1
    # if mysqld is not running, trigger immediate failure
    check_mysql_procs
    if [ $? -eq 0 ]; then
      action $"Starting $prog: " /bin/false
      return 1
    fi

		let STARTTIMEOUT=${STARTTIMEOUT}-1
	    done
	    if [ $STARTTIMEOUT -eq 0 ]; then
                    echo "Timeout error occurred trying to start MySQL Daemon."
                    action $"Starting $prog: " /bin/false
                    ret=1
            else
                    action $"Starting $prog: " /bin/true
            fi
	else
	    action $"Starting $prog: " /bin/false
	fi
	[ $ret -eq 0 ] && touch /var/lock/subsys/mysqld
	return $ret
}

stop(){
        MYSQLPID=`cat "$mypidfile"  2>/dev/null `
        if [ -n "$MYSQLPID" ]; then
            /bin/kill "$MYSQLPID" >/dev/null 2>&1
            ret=$?
            if [ $ret -eq 0 ]; then
                STOPTIMEOUT=600
                while [ $STOPTIMEOUT -gt 0 ]; do
                    /bin/kill -0 "$MYSQLPID" >/dev/null 2>&1 || break
                    sleep 1
                    let STOPTIMEOUT=${STOPTIMEOUT}-1
                done
                if [ $STOPTIMEOUT -eq 0 ]; then
                    echo "Timeout error occurred trying to stop MySQL Daemon."
                    ret=1
                    action $"Stopping $prog: " /bin/false
                else
                    rm -f /var/lock/subsys/mysqld
                    rm -f "$socketfile"
                    action $"Stopping $prog: " /bin/true
                fi
            else
                action $"Stopping $prog: " /bin/false
            fi
        else
            ret=1
            action $"Stopping $prog: " /bin/false
        fi
        return $ret
}

restart(){
    stop
    start
}

condrestart(){
    [ -e /var/lock/subsys/mysqld ] && restart || :
}

# See how we were called.
case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  status)
    status mysqld
    ;;
  restart)
    restart
    ;;
  condrestart)
    condrestart
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|condrestart|restart}"
    exit 1
esac

exit $?
