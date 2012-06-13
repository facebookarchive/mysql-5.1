#
# Do "set read_only=0|1 on tables with concurrent inserts to tables with
# autoinc columns. The set might force a flush of the table cache and
# we need to confirm that INSERT_ID is handled correctly.
#

let $innodb_file_format_check_orig=`select @@innodb_file_format_check`;

# create the directory for temporary log files.
--exec rm -rf $MYSQL_TMP_DIR/innodb_autoinc_flush
--exec mkdir $MYSQL_TMP_DIR/innodb_autoinc_flush

--echo Insert 1000 rows
--disable_query_log
let $x = 1000;
while ($x) {
  eval INSERT INTO t1 values (NULL, -1, $x);
  dec $x;
}
--enable_query_log

let $pid_file = `SELECT @@pid_file`;

set global read_only=1;

let $exec =
python suite/innodb_plugin/t/innodb_autoinc_flush.py $pid_file $MYSQL_TMP_DIR/innodb_autoinc_flush
$num_workers $test_seconds root 127.0.0.1 $MASTER_MYPORT;

exec $exec;

--echo Checksum master
let $master_checksum = query_get_value(CHECKSUM TABLE t1, Checksum, 1);

connection slave;

let $slave_sync_timeout= 1200;
--source include/wait_for_slave_to_sync_with_master.inc

--echo Checksum slave
let $slave_checksum=query_get_value(CHECKSUM TABLE t1, Checksum, 1);

let $not_same = `SELECT $master_checksum-$slave_checksum`;
if ($not_same)
{
  echo "checksum mismatch"

  connection master;
  eval select * into outfile '$MYSQLTEST_VARDIR/tmp/master_all' from t1 order by id;
  eval select id into outfile '$MYSQLTEST_VARDIR/tmp/master_id' from t1 order by id;
  show master status;

  connection slave;
  eval select * into outfile '$MYSQLTEST_VARDIR/tmp/slave_all' from t1 order by id;
  eval select id into outfile '$MYSQLTEST_VARDIR/tmp/slave_id' from t1 order by id;
  show slave status;

  die;
}

connection master;
set global read_only=0;
DROP TABLE t1;
sync_slave_with_master;

connection master;
eval set global innodb_file_format_check=$innodb_file_format_check_orig;
