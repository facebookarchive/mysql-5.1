# Do inserts, updates, deletes concurrent with DROP TABLE

let $innodb_file_format_check_orig=`select @@innodb_file_format_check`;

# create the directory for temporary log files.
--exec rm -rf $MYSQL_TMP_DIR/innodb_dropper
--exec mkdir $MYSQL_TMP_DIR/innodb_dropper

call mtr.add_suppression("InnoDB: Warning: Small buffer pool size.*");
call mtr.add_suppression("InnoDB: Error: trying to do i/o to a tablespace which does not exist");
call mtr.add_suppression("InnoDB: Error: trying to do ibuf merge to a dropped tablespace.*");

let $pid_file = `SELECT @@pid_file`;

let $exec =
python suite/innodb_plugin/t/table_dropper.py $pid_file $MYSQL_TMP_DIR/innodb_dropper
$min_records $max_records $num_workers $test_seconds $use_blob root 127.0.0.1 $MASTER_MYPORT;

exec $exec;

eval set global innodb_file_format_check=$innodb_file_format_check_orig;
