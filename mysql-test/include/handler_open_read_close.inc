# include/handler_open_read_close.inc
#
# The variables
#     $engine_type       -- storage engine to be tested
#     $other_engine_type -- storage engine <> $engine_type
#     $other_handler_engine_type -- storage engine <> $engine_type, if possible
#                           1. $other_handler_engine_type must support handler
#                           2. $other_handler_engine_type must point to an all
#                              time available storage engine
#                              2006-08 MySQL 5.1 MyISAM and MEMORY only
# have to be set before sourcing this script.
-- source include/not_embedded.inc
#
# test of HANDLER OPEN_READ_CLOSE ...

eval SET SESSION STORAGE_ENGINE = $engine_type;

--disable_warnings
drop table if exists t1,t3,t4,t5;
--enable_warnings

create table t1 (a int, b char(10), key a(a), key b(a,b));
insert into t1 values
(17,"ddd"),(18,"eee"),(19,"fff"),(19,"yyy"),
(14,"aaa"),(15,"bbb"),(16,"ccc"),(16,"xxx"),
(20,"ggg"),(21,"hhh"),(22,"iii");

#
# Testing handler open_read_close
#
-- error ER_PARSE_ERROR
handler t1 open_read_close a=(SELECT 1);
handler t1 open_read_close a first;
handler t1 open_read_close a last;

handler t1 open_read_close a first;

handler t1 open_read_close a last;

handler t1 open_read_close a=(15);
handler t1 open_read_close a=(16);

--error ER_TOO_MANY_KEY_PARTS
handler t1 open_read_close a=(19,"fff");

handler t1 open_read_close b=(19,"fff");
handler t1 open_read_close b=(19,"yyy");
handler t1 open_read_close b=(19);

handler t1 open_read_close a last;

handler t1 open_read_close a=(11);
handler t1 open_read_close a>=(11);

handler t1 open_read_close a=(18);
handler t1 open_read_close a>=(18);
handler t1 open_read_close a>(18);
handler t1 open_read_close a<=(18);
handler t1 open_read_close a<(18);

handler t1 open_read_close a first limit 5;

handler t1 open_read_close a>=(16) limit 4;
handler t1 open_read_close a>=(16) limit 2,2;
handler t1 open_read_close a last  limit 3;

handler t1 open_read_close a=(19);
handler t1 open_read_close a=(19) where b="yyy";

handler t1 open_read_close first;
--error ER_PARSE_ERROR
handler t1 open_read_close last;

drop table t1;
