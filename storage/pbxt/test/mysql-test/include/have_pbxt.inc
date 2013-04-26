disable_query_log;
--require r/true.require
select support = 'DEFAULT' or support = 'YES' as `TRUE` from information_schema.engines where engine = 'pbxt';
enable_query_log;
