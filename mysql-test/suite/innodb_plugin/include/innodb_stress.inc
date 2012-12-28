# Populate a table with 1000 records. Allow the replica to sync with the master.
# Run concurrent threads that run OLTP transactions on master.
# Kill the master database server at random points.
# Check the table against the replica.
# Reinvoke the threads.

# create the directory for temporary log files.
--exec mkdir -p $MYSQL_TMP_DIR/load_generator

--connection master
SELECT @@innodb_file_format_check;

--disable_warnings
drop table if exists errors;
--enable_warnings
create table errors(msg text) engine=innodb;

--connection slave
SELECT @@innodb_file_format_check;

--connection master

# since this test generates lot of errors in log, suppress checking errors
call mtr.add_suppression(".*");

--sync_slave_with_master

--connection master
--let $pid_file = `SELECT @@pid_file`
--let $crash_num = 0
--let $master_host = 127.0.0.1
--let $table = test
--let $user = root
--disable_query_log
--let $zlib_wrap_save = `SELECT @@innodb_zlib_wrap`
--enable_query_log

while ($num_crashes)
{
  connection master;
  exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect;
  if ($crash_num)
  {
    let $num_records = 0; # do not populate the table except for the first run.
  }

  if ($use_blob)
  {
    let $exec =
python suite/innodb_plugin/t/load_generator.py $pid_file $kill_db_after
$num_records  $num_workers $num_transactions $user $master_host $MASTER_MYPORT
$table 1 $max_rows $MYSQL_TMP_DIR/load_generator;
  }
  if (!$use_blob)
  {
    let $exec =
python suite/innodb_plugin/t/load_generator.py $pid_file $kill_db_after
$num_records  $num_workers $num_transactions $user $master_host $MASTER_MYPORT
$table 0 $max_rows $MYSQL_TMP_DIR/load_generator;
  }

  exec $exec;

  if ($do_crash)
  {
    --echo Wait for reconnect
    enable_reconnect;
    # Call script that will poll the server waiting for it to be back online again
    source include/wait_until_connected_again.inc;
    connection slave;
    source include/wait_until_connected_again.inc;
    connection master;
  }

  --echo Checksum master
  let $master_checksum = query_get_value(CHECKSUM TABLE t1, Checksum, 1);

  # if sync_slave_with_master had a configurable timeout this would not be needed
  let $slave_sync_timeout = 7200;
  --source include/wait_for_slave_to_sync_with_master.inc

  connection slave;
  --echo Checksum slave
  let $slave_checksum=query_get_value(CHECKSUM TABLE t1, Checksum, 1);
  let $not_same = `SELECT $master_checksum-$slave_checksum`;
  if ($not_same)
  {
    let $msg =
The checksums of table t1 for master and slave do not match for $crash_num th
crash. This may happen if there is a corrupt recovery log or a bug in crash
recovery. You can take a look at the logs in $MYSQL_TMP_DIR/load_generator to see the
queries issued before the crash.;
    echo $msg;

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
  dec $num_crashes;
  inc $crash_num;
}

# final cleanup
--connection slave
SELECT @@innodb_file_format_check;

--connection master
SELECT @@innodb_file_format_check;

select * from errors limit 1000;

DROP TABLE errors;
DROP TABLE t1;
sync_slave_with_master;

--connection slave
stop slave;

connection master;

# restore innodb_file_format and innodb_file_format_check
# Wish we did not have to waste time on this -- http://bugs.mysql.com/bug.php?id=62075
SET GLOBAL innodb_file_format_check = "Antelope";
--disable_query_log
eval SET GLOBAL innodb_zlib_wrap=$zlib_wrap_save;
--enable_query_log
# --exec rm -rf $MYSQL_TMP_DIR/load_generator
