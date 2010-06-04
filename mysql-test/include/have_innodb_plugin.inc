disable_query_log;
--require r/true.require
SELECT TRUE FROM information_schema.plugins WHERE LOWER(plugin_name) = 'innodb_cmp' AND LOWER(plugin_status) = 'active';
enable_query_log;
