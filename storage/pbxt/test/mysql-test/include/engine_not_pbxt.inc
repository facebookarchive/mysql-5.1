--require r/true.require
disable_query_log;
select convert(@@table_type using latin1) != "PBXT" as "TRUE";
enable_query_log;
