disable_query_log;
--require r/true.require
select TRUE from information_schema.plugins where PLUGIN_NAME='InnoDB';
enable_query_log;
