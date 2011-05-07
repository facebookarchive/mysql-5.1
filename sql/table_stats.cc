
#include "mysql_priv.h"
#include "my_atomic.h"

HASH global_table_stats;
static pthread_mutex_t LOCK_global_table_stats;

/*
  Update global table statistics for this table and optionally tables
  linked via TABLE::next.

  SYNOPSIS
    update_table_stats()
      tablep - the table for which global table stats are updated
      follow_next - when TRUE, update global stats for tables linked
                    via TABLE::next
 */
void update_table_stats(THD *thd, TABLE *tablep, bool follow_next)
{
  for (; tablep; tablep= tablep->next)
  {
    if (tablep->file)
      tablep->file->update_global_table_stats(thd);

    if (!follow_next)
      return;
  }
}

static void
clear_table_stats_counters(TABLE_STATS* table_stats)
{
  int x;

  for (x=0; x < MAX_INDEX_STATS; ++x)
  {
    table_stats->indexes[x].rows_inserted= 0;
    table_stats->indexes[x].rows_updated= 0;
    table_stats->indexes[x].rows_deleted= 0;
    table_stats->indexes[x].rows_read= 0;
    table_stats->indexes[x].rows_requested= 0;
    table_stats->indexes[x].rows_index_first= 0;
    table_stats->indexes[x].rows_index_next= 0;
    my_io_perf_init(&(table_stats->indexes[x].io_perf_read));
  }

  table_stats->rows_inserted= 0;
  table_stats->rows_updated= 0;
  table_stats->rows_deleted= 0;
  table_stats->rows_read= 0;
  table_stats->rows_requested= 0;
  table_stats->rows_index_first= 0;
  table_stats->rows_index_next= 0;

  my_io_perf_init(&table_stats->io_perf_read);
  my_io_perf_init(&table_stats->io_perf_write);
  table_stats->index_inserts = 0;
}

/*
  Initialize the index names in table_stats->indexes

  SYNOPSIS
    set_index_stats_names
    table_stats - object to initialize

  RETURN VALUE
    0 on success, !0 on failure

  Stats are stored for at most MAX_INDEX_KEYS and when there are more than
  (MAX_INDEX_KEYS-1) indexes then use the last entry for the extra indexes
  which gets the name "STATS_OVERFLOW".
*/
static int
set_index_stats_names(TABLE_STATS *table_stats, TABLE *table)
{
  uint x;

  table_stats->num_indexes= min(table->s->keys, MAX_INDEX_STATS);

  for (x=0; x < table_stats->num_indexes; ++x)
  {
    char const *index_name = table->s->key_info[x].name;

    if (x == (MAX_INDEX_STATS - 1) && table->s->keys > MAX_INDEX_STATS)
      index_name = "STATS_OVERFLOW";
 
    if (snprintf(table_stats->indexes[x].name, NAME_LEN+1, "%s",
                 index_name) < 0)
    {
      return -1;
    }
  }

  return 0;
}

/*
  Return the global TABLE_STATS object for a table.

  SYNOPSIS
    get_table_stats()
    table          in: table for which an object is returned
    type_of_db     in: storage engine type

  RETURN VALUE
    TABLE_STATS structure for the requested table
    NULL on failure
*/
TABLE_STATS *
get_table_stats(TABLE *table, handlerton *engine_type)
{
  TABLE_STATS* table_stats;

  if (!table->s || !table->s->db.str || !table->s->table_name.str ||
      !table->s->table_cache_key.str || !table->s->table_cache_key.length)
  {
    sql_print_error("No key for table stats.");
    return NULL;
  }

  pthread_mutex_lock(&LOCK_global_table_stats);

  // Gets or creates the TABLE_STATS object for this table.
  if (!(table_stats= (TABLE_STATS*)hash_search(&global_table_stats,
                                               (uchar*)table->s->table_cache_key.str,
                                               table->s->table_cache_key.length)))
  {
    if (!(table_stats= ((TABLE_STATS*)my_malloc(sizeof(TABLE_STATS),
                                                MYF(MY_WME)))))
    {
      sql_print_error("Cannot allocate memory for TABLE_STATS.");
      pthread_mutex_unlock(&LOCK_global_table_stats);
      return NULL;
    }

    DBUG_ASSERT(table->s->table_cache_key.length <= (NAME_LEN * 2 + 2));
    memcpy(table_stats->hash_key, table->s->table_cache_key.str, table->s->table_cache_key.length);
    table_stats->hash_key_len= table->s->table_cache_key.length;

    if (snprintf(table_stats->db, NAME_LEN+1, "%s", table->s->db.str) < 0 ||
        snprintf(table_stats->table, NAME_LEN+1, "%s",
                 table->s->table_name.str) < 0 ||
        snprintf(table_stats->db_table, NAME_LEN * 2 + 2, "%s.%s",
                 table->s->db.str, table->s->table_name.str) < 0)
    {
      sql_print_error("Cannot generate name for table stats.");
      my_free((char*)table_stats, 0);
      pthread_mutex_unlock(&LOCK_global_table_stats);
      return NULL;
    }
    table_stats->db_table_len= strlen(table_stats->db_table);

    if (set_index_stats_names(table_stats, table))
    {
      sql_print_error("Cannot generate name for index stats.");
      my_free((char*)table_stats, 0);
      pthread_mutex_unlock(&LOCK_global_table_stats);
      return NULL;
    }

    clear_table_stats_counters(table_stats);
    table_stats->engine_type= engine_type;

    if (my_hash_insert(&global_table_stats, (uchar*)table_stats))
    {
      // Out of memory.
      sql_print_error("Inserting table stats failed.");
      my_free((char*)table_stats, 0);
      pthread_mutex_unlock(&LOCK_global_table_stats);
      return NULL;
    }
  }
  else
  {
    /*
      Keep things in sync after create or drop index. This doesn't notice create
      followed by drop. "reset statistics" will fix that.
    */
    if (table_stats->num_indexes != min(table->s->keys, MAX_INDEX_STATS))
    {
      if (set_index_stats_names(table_stats, table))
      {
        sql_print_error("Cannot generate name for index stats.");
        pthread_mutex_unlock(&LOCK_global_table_stats);
        return NULL;
      }
    }
  }

  pthread_mutex_unlock(&LOCK_global_table_stats);

  return table_stats;
}
  
extern "C" uchar *get_key_table_stats(TABLE_STATS *table_stats, size_t *length,
                                      my_bool not_used __attribute__((unused)))
{
  *length = table_stats->hash_key_len;
  return (uchar*)table_stats->hash_key;
}

extern "C" void free_table_stats(TABLE_STATS* table_stats)
{
  my_free((char*)table_stats, MYF(0));
}

void init_global_table_stats(void)
{
  pthread_mutex_init(&LOCK_global_table_stats, MY_MUTEX_INIT_FAST);
  if (hash_init(&global_table_stats, system_charset_info, max_connections,
                0, 0, (hash_get_key)get_key_table_stats,
                (hash_free_key)free_table_stats, 0)) {
    sql_print_error("Initializing global_table_stats failed.");
    unireg_abort(1);
  }
}

void free_global_table_stats(void)
{
  hash_free(&global_table_stats);
  pthread_mutex_destroy(&LOCK_global_table_stats);
}

void reset_global_table_stats()
{
  pthread_mutex_lock(&LOCK_global_table_stats);

  for (unsigned i = 0; i < global_table_stats.records; ++i) {
    TABLE_STATS *table_stats =
      (TABLE_STATS*)hash_element(&global_table_stats, i);

    clear_table_stats_counters(table_stats);

    /*
      The next caller to get_table_stats will reset this. It isn't
      done here because the TABLE object is required to determine
      the index names. This can be called after drop/add index is
      done where the table has the same number of indexes but
      different index names after the DDL.
    */
    table_stats->num_indexes= 0;
  }

  pthread_mutex_unlock(&LOCK_global_table_stats);
}

ST_FIELD_INFO table_stats_fields_info[]=
{
  {"TABLE_SCHEMA", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"TABLE_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"TABLE_ENGINE", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"ROWS_INSERTED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_UPDATED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_DELETED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_READ", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_REQUESTED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},

  {"ROWS_INDEX_FIRST", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_INDEX_NEXT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},

  {"IO_READ_BYTES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_REQUESTS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_SVC_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_SVC_USECS_MAX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_WAIT_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_WAIT_USECS_MAX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_OLD_IOS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},

  {"IO_WRITE_BYTES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_WRITE_REQUESTS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_WRITE_SVC_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_WRITE_SVC_USECS_MAX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_WRITE_WAIT_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_WRITE_WAIT_USECS_MAX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_WRITE_OLD_IOS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},

  {"IO_INDEX_INSERTS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0}
};

int fill_table_stats(THD *thd, TABLE_LIST *tables, COND *cond)
{
  DBUG_ENTER("fill_table_stats");
  TABLE* table= tables->table;

  pthread_mutex_lock(&LOCK_global_table_stats);

  for (unsigned i = 0; i < global_table_stats.records; ++i) {
    int f= 0;

    TABLE_STATS *table_stats =
      (TABLE_STATS*)hash_element(&global_table_stats, i);

    if (table_stats->rows_inserted == 0 &&
        table_stats->rows_updated == 0 &&
        table_stats->rows_deleted == 0 &&
        table_stats->rows_read == 0 &&
        table_stats->rows_requested == 0)
    {
      continue;
    }

    restore_record(table, s->default_values);
    table->field[f++]->store(table_stats->db, strlen(table_stats->db),
                           system_charset_info);
    table->field[f++]->store(table_stats->table, strlen(table_stats->table),
                             system_charset_info);

    // TODO -- this can be optimized in the future
    const char* engine= ha_resolve_storage_engine_name(table_stats->engine_type);
    table->field[f++]->store(engine, strlen(engine), system_charset_info);

    table->field[f++]->store(table_stats->rows_inserted, TRUE);
    table->field[f++]->store(table_stats->rows_updated, TRUE);
    table->field[f++]->store(table_stats->rows_deleted, TRUE);
    table->field[f++]->store(table_stats->rows_read, TRUE);
    table->field[f++]->store(table_stats->rows_requested, TRUE);

    table->field[f++]->store(table_stats->rows_index_first, TRUE);
    table->field[f++]->store(table_stats->rows_index_next, TRUE);

    table->field[f++]->store(table_stats->io_perf_read.bytes, TRUE);
    table->field[f++]->store(table_stats->io_perf_read.requests, TRUE);
    table->field[f++]->store(table_stats->io_perf_read.svc_usecs, TRUE);
    table->field[f++]->store(table_stats->io_perf_read.svc_usecs_max, TRUE);
    table->field[f++]->store(table_stats->io_perf_read.wait_usecs, TRUE);
    table->field[f++]->store(table_stats->io_perf_read.wait_usecs_max, TRUE);
    table->field[f++]->store(table_stats->io_perf_read.old_ios, TRUE);

    table->field[f++]->store(table_stats->io_perf_write.bytes, TRUE);
    table->field[f++]->store(table_stats->io_perf_write.requests, TRUE);
    table->field[f++]->store(table_stats->io_perf_write.svc_usecs, TRUE);
    table->field[f++]->store(table_stats->io_perf_write.svc_usecs_max, TRUE);
    table->field[f++]->store(table_stats->io_perf_write.wait_usecs, TRUE);
    table->field[f++]->store(table_stats->io_perf_write.wait_usecs_max, TRUE);
    table->field[f++]->store(table_stats->io_perf_write.old_ios, TRUE);

    table->field[f++]->store(table_stats->index_inserts, TRUE);

    if (schema_table_store_record(thd, table))
    {
      pthread_mutex_unlock(&LOCK_global_table_stats);
      DBUG_RETURN(-1);
    }
  }
  pthread_mutex_unlock(&LOCK_global_table_stats);

  DBUG_RETURN(0);
}

ST_FIELD_INFO index_stats_fields_info[]=
{
  {"TABLE_SCHEMA", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"TABLE_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"INDEX_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"TABLE_ENGINE", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"ROWS_INSERTED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_UPDATED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_DELETED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_READ", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_REQUESTED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},

  {"ROWS_INDEX_FIRST", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_INDEX_NEXT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},

  {"IO_READ_BYTES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_REQUESTS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_SVC_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_SVC_USECS_MAX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_WAIT_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_WAIT_USECS_MAX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"IO_READ_OLD_IOS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0}
};


int fill_index_stats(THD *thd, TABLE_LIST *tables, COND *cond)
{
  DBUG_ENTER("fill_index_stats");
  TABLE* table= tables->table;

  pthread_mutex_lock(&LOCK_global_table_stats);

  for (unsigned i = 0; i < global_table_stats.records; ++i) {
    uint ix;

    TABLE_STATS *table_stats =
      (TABLE_STATS*)hash_element(&global_table_stats, i);

    for (ix=0; ix < table_stats->num_indexes; ++ix)
    {
      INDEX_STATS *index_stats= &(table_stats->indexes[ix]); 
      int f= 0;

      if (index_stats->rows_inserted == 0 &&
          index_stats->rows_updated == 0 &&
          index_stats->rows_deleted == 0 &&
          index_stats->rows_read == 0 &&
          index_stats->rows_requested == 0)
      {
        continue;
      }

      restore_record(table, s->default_values);

      table->field[f++]->store(table_stats->db, strlen(table_stats->db),
                               system_charset_info);
      table->field[f++]->store(table_stats->table, strlen(table_stats->table),
                               system_charset_info);
      table->field[f++]->store(index_stats->name, strlen(index_stats->name),
                               system_charset_info);

      // TODO -- this can be optimized in the future
      const char* engine= ha_resolve_storage_engine_name(table_stats->engine_type);
      table->field[f++]->store(engine, strlen(engine), system_charset_info);

      table->field[f++]->store(index_stats->rows_inserted, TRUE);
      table->field[f++]->store(index_stats->rows_updated, TRUE);
      table->field[f++]->store(index_stats->rows_deleted, TRUE);
      table->field[f++]->store(index_stats->rows_read, TRUE);
      table->field[f++]->store(index_stats->rows_requested, TRUE);

      table->field[f++]->store(index_stats->rows_index_first, TRUE);
      table->field[f++]->store(index_stats->rows_index_next, TRUE);

      table->field[f++]->store(index_stats->io_perf_read.bytes, TRUE);
      table->field[f++]->store(index_stats->io_perf_read.requests, TRUE);
      table->field[f++]->store(index_stats->io_perf_read.svc_usecs, TRUE);
      table->field[f++]->store(index_stats->io_perf_read.svc_usecs_max, TRUE);
      table->field[f++]->store(index_stats->io_perf_read.wait_usecs, TRUE);
      table->field[f++]->store(index_stats->io_perf_read.wait_usecs_max, TRUE);
      table->field[f++]->store(index_stats->io_perf_read.old_ios, TRUE);

      if (schema_table_store_record(thd, table))
      {
        pthread_mutex_unlock(&LOCK_global_table_stats);
        DBUG_RETURN(-1);
      }
    }
  }

  pthread_mutex_unlock(&LOCK_global_table_stats);

  DBUG_RETURN(0);
}

extern "C" {
void async_update_table_stats(
	struct st_table_stats* table_stats, /* in: table stats structure */
	my_bool		write,          /* in: true if this is a write operation */
	longlong	bytes,		/* in: size of request */
	double		svc_secs,	/* in: secs to perform IO */
	my_fast_timer_t* stop_timer,	/* in: timer for now */
	my_fast_timer_t* wait_start,	/* in: timer when IO request submitted */
	my_bool		old_io)		/* in: true if IO exceeded age threshold */
{
	longlong svc_usecs = (longlong)(1000000 * svc_secs);

	double	wait_secs = my_fast_timer_diff(wait_start, stop_timer);
	longlong wait_usecs = (longlong)(1000000 * wait_secs);

	my_io_perf_t* perf = write ? &table_stats->io_perf_write :
				     &table_stats->io_perf_read;

        my_io_perf_sum_atomic(perf, bytes, /*requests*/ 1, svc_usecs,
                              wait_usecs, old_io);
}

} // extern "C"

ST_FIELD_INFO user_stats_fields_info[]=
{
  {"USER_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, 0},
  {"BINLOG_BYTES_WRITTEN", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"BYTES_RECEIVED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"BYTES_SENT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_DDL", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_DELETE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_HANDLER", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_INSERT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_OTHER", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_SELECT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_TRANSACTION", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"COMMANDS_UPDATE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"CONNECTIONS_CONCURRENT", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0, 0, 0},
  {"CONNECTIONS_DENIED_MAX_GLOBAL", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"CONNECTIONS_DENIED_MAX_USER", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"CONNECTIONS_LOST", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"CONNECTIONS_TOTAL", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"DISK_READ_BYTES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"DISK_READ_REQUESTS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"DISK_READ_SVC_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"DISK_READ_WAIT_USECS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ERRORS_ACCESS_DENIED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ERRORS_TOTAL", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"MICROSECONDS_CPU", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"MICROSECONDS_RECORDS_IN_RANGE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"MICROSECONDS_WALL", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"QUERIES_EMPTY", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"RECORDS_IN_RANGE_CALLS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_DELETED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_FETCHED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_INSERTED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_READ", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_UPDATED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_INDEX_FIRST", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"ROWS_INDEX_NEXT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"TRANSACTIONS_COMMIT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"TRANSACTIONS_ROLLBACK", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0},
  {"QUERIES_RUNNING", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0, 0, 0},
  {"QUERIES_WAITING", MY_INT32_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0, 0, 0},
  {0, 0, MYSQL_TYPE_STRING, 0, 0, 0}
};

