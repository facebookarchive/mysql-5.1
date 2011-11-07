/* Copyright (C) 2000-2006 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


/* The old structures from unireg */

struct st_table;
class Field;

typedef struct st_date_time_format {
  uchar positions[8];
  char  time_separator;			/* Separator between hour and minute */
  uint flag;				/* For future */
  LEX_STRING format;
} DATE_TIME_FORMAT;


typedef struct st_keyfile_info {	/* used with ha_info() */
  uchar ref[MAX_REFLENGTH];		/* Pointer to current row */
  uchar dupp_ref[MAX_REFLENGTH];	/* Pointer to dupp row */
  uint ref_length;			/* Length of ref (1-8) */
  uint block_size;			/* index block size */
  File filenr;				/* (uniq) filenr for table */
  ha_rows records;			/* Records i datafilen */
  ha_rows deleted;			/* Deleted records */
  ulonglong data_file_length;		/* Length off data file */
  ulonglong max_data_file_length;	/* Length off data file */
  ulonglong index_file_length;
  ulonglong max_index_file_length;
  ulonglong delete_length;		/* Free bytes */
  ulonglong auto_increment_value;
  int errkey,sortkey;			/* Last errorkey and sorted by */
  time_t create_time;			/* When table was created */
  time_t check_time;
  time_t update_time;
  ulong mean_rec_length;		/* physical reclength */
} KEYFILE_INFO;


typedef struct st_key_part_info {	/* Info about a key part */
  Field *field;
  uint	offset;				/* offset in record (from 0) */
  uint	null_offset;			/* Offset to null_bit in record */
  uint16 length;                        /* Length of keypart value in bytes */
  /* 
    Number of bytes required to store the keypart value. This may be
    different from the "length" field as it also counts
     - possible NULL-flag byte (see HA_KEY_NULL_LENGTH)
     - possible HA_KEY_BLOB_LENGTH bytes needed to store actual value length.
  */
  uint16 store_length;
  uint16 key_type;
  uint16 fieldnr;			/* Fieldnum in UNIREG */
  uint16 key_part_flag;			/* 0 or HA_REVERSE_SORT */
  uint8 type;
  uint8 null_bit;			/* Position to null_bit */
} KEY_PART_INFO ;


typedef struct st_key {
  uint	key_length;			/* Tot length of key */
  ulong flags;                          /* dupp key and pack flags */
  uint	key_parts;			/* How many key_parts */
  uint  extra_length;
  uint	usable_key_parts;		/* Should normally be = key_parts */
  uint  block_size;
  enum  ha_key_alg algorithm;
  /*
    Note that parser is used when the table is opened for use, and
    parser_name is used when the table is being created.
  */
  union
  {
    plugin_ref parser;                  /* Fulltext [pre]parser */
    LEX_STRING *parser_name;            /* Fulltext [pre]parser name */
  };
  KEY_PART_INFO *key_part;
  char	*name;				/* Name of key */
  /*
    Array of AVG(#records with the same field value) for 1st ... Nth key part.
    0 means 'not known'.
    For temporary heap tables this member is NULL.
  */
  ulong *rec_per_key;
  union {
    int  bdb_return_if_eq;
  } handler;
  struct st_table *table;
} KEY;


struct st_join_table;

typedef struct st_reginfo {		/* Extra info about reg */
  struct st_join_table *join_tab;	/* Used by SELECT() */
  enum thr_lock_type lock_type;		/* How database is used */
  bool not_exists_optimize;
  /*
    TRUE <=> range optimizer found that there is no rows satisfying
    table conditions.
  */
  bool impossible_range;
} REGINFO;


class SQL_SELECT;
class THD;
class handler;
struct st_join_table;

void rr_unlock_row(st_join_table *tab);

struct READ_RECORD {			/* Parameter to read_record */
  typedef int (*Read_func)(READ_RECORD*);
  typedef void (*Unlock_row_func)(st_join_table *);
  struct st_table *table;			/* Head-form */
  handler *file;
  struct st_table **forms;			/* head and ref forms */

  Read_func read_record;
  Unlock_row_func unlock_row;
  THD *thd;
  SQL_SELECT *select;
  uint cache_records;
  uint ref_length,struct_length,reclength,rec_cache_size,error_offset;
  uint index;
  uchar *ref_pos;				/* pointer to form->refpos */
  uchar *record;
  uchar *rec_buf;                /* to read field values  after filesort */
  uchar	*cache,*cache_pos,*cache_end,*read_positions;
  IO_CACHE *io_cache;
  bool print_error, ignore_not_found_rows;
};


/*
  Originally MySQL used MYSQL_TIME structure inside server only, but since
  4.1 it's exported to user in the new client API. Define aliases for
  new names to keep existing code simple.
*/

typedef enum enum_mysql_timestamp_type timestamp_type;


typedef struct {
  ulong year,month,day,hour;
  ulonglong minute,second,second_part;
  bool neg;
} INTERVAL;


typedef struct st_known_date_time_format {
  const char *format_name;
  const char *date_format;
  const char *datetime_format;
  const char *time_format;
} KNOWN_DATE_TIME_FORMAT;

enum SHOW_COMP_OPTION { SHOW_OPTION_YES, SHOW_OPTION_NO, SHOW_OPTION_DISABLED};

extern const char *show_comp_option_name[];

typedef int *(*update_var)(THD *, struct st_mysql_show_var *);

typedef struct	st_lex_user {
  LEX_STRING user, host, password;
} LEX_USER;

/*
  This structure specifies the maximum amount of resources which
  can be consumed by each account. Zero value of a member means
  there is no limit.
*/
typedef struct user_resources {
  /* Maximum number of queries/statements per hour. */
  uint questions;
  /*
     Maximum number of updating statements per hour (which statements are
     updating is defined by sql_command_flags array).
  */
  uint updates;
  /* Maximum number of connections established per hour. */
  uint conn_per_hour;
  /* Maximum number of concurrent connections. */
  uint user_conn;
  /* Maximum number of concurrent queries. */
  int max_concurrent_queries;
  /* Maximum number of concurrent transactions. */
  int max_concurrent_transactions;
  /*
     Values of this enum and specified_limits member are used by the
     parser to store which user limits were specified in GRANT statement.
  */
  enum {QUERIES_PER_HOUR= 1, UPDATES_PER_HOUR= 2, CONNECTIONS_PER_HOUR= 4,
        USER_CONNECTIONS= 8, USER_CONCURRENT_QUERIES= 16,
        USER_CONCURRENT_TRANSACTIONS= 32};
  uint specified_limits;
} USER_RESOURCES;

#define USER_STATS_MAGIC 0x17171717

/** Counts resources consumed per-user.
    Data is exported via IS.user_statistics.
*/

typedef struct st_user_stats {
  my_io_perf_t     io_perf_read;
  my_atomic_bigint bytes_received;
  my_atomic_bigint bytes_sent;
  my_atomic_bigint binlog_bytes_written;
  my_atomic_bigint commands_ddl;
  my_atomic_bigint commands_delete;
  my_atomic_bigint commands_handler;
  my_atomic_bigint commands_insert;
  my_atomic_bigint commands_other;
  my_atomic_bigint commands_select;
  my_atomic_bigint commands_transaction;
  my_atomic_bigint commands_update;
  my_atomic_bigint connections_denied_max_global; // global limit exceeded
  my_atomic_bigint connections_denied_max_user;   // per user limit exceeded
  my_atomic_bigint connections_lost;              // closed on error
  my_atomic_bigint connections_total;             // total conns created
  my_atomic_bigint errors_access_denied;          // denied access to table or db
  my_atomic_bigint errors_total;
  my_atomic_bigint keys_dirtied;                  // number of memcache keys dirtied
  my_atomic_bigint limit_wait_queries;
  my_atomic_bigint limit_fail_transactions;
  my_atomic_bigint microseconds_cpu;
  my_atomic_bigint microseconds_records_in_range;
  my_atomic_bigint microseconds_wall;
  my_atomic_bigint microseconds_ddl;
  my_atomic_bigint microseconds_delete;
  my_atomic_bigint microseconds_handler;
  my_atomic_bigint microseconds_insert;
  my_atomic_bigint microseconds_other;
  my_atomic_bigint microseconds_select;
  my_atomic_bigint microseconds_transaction;
  my_atomic_bigint microseconds_update;
  my_atomic_bigint queries_empty;
  my_atomic_bigint records_in_range_calls;
  my_atomic_bigint rows_deleted;
  my_atomic_bigint rows_fetched;
  my_atomic_bigint rows_inserted;
  my_atomic_bigint rows_read;
  my_atomic_bigint rows_updated;

  /* see variables of same name in ha_statistics */
  my_atomic_bigint volatile rows_index_first;
  my_atomic_bigint volatile rows_index_next;

  my_atomic_bigint transactions_commit;
  my_atomic_bigint transactions_rollback;

  uint magic;

  /* TODO(mcallaghan) -- failed_queries, disk IO, parse and records_in_range
     seconds, slow queries. I also want to count connections that fail
     authentication but the hash_user_connections key is (user,host) and when
     auth fails you know which user/host the login provided but you don't know
     which pair it wanted to use. Read the docs for how auth uses mysql.user
     table for more details. When auth failure occurs mysqld doesn't have
     a referenced to a USER_STATS entry. This probably requires another hash
     table keyed only by the login name.
     Others:
       errors_lock_wait_timeout, errors_deadlock
       queries_slow
  */
} USER_STATS;


/*
  This structure is used for counting resources consumed and for checking
  them against specified user limits.
*/
typedef struct  user_conn {
  /*
     Pointer to user+host key (pair separated by '\0') defining the entity
     for which resources are counted (By default it is user account thus
     priv_user/priv_host pair is used. If --old-style-user-limits option
     is enabled, resources are counted for each user+host separately).
  */
  char *user;
  /* Pointer to host part of the key. */
  char *host;
  /**
     The moment of time when per hour counters were reset last time
     (i.e. start of "hour" for conn_per_hour, updates, questions counters).
  */
  ulonglong reset_utime;
  /* Total length of the key. */
  uint len;
  /* Current amount of concurrent connections for this account. */
  uint connections;
  /*
     Current number of connections per hour, number of updating statements
     per hour and total number of statements per hour for this account.
  */
  uint conn_per_hour, updates, questions;

  /* Tracking variables for admission control.  Tracks the number of
   * running and waiting queries.  */
  volatile int32    queries_running; /* changed by atomic inc */
  volatile int32    queries_waiting; /* protected by query_mutex */

  /* Condvar used to block when waiting for the user to be able to
   * start a new query, and signal when a new query can begin.
   * query_mutex guards queries_waiting. */
  pthread_mutex_t   query_mutex;
  pthread_cond_t    query_condvar;

  volatile int32    tx_slots_inuse; /* changed by atomic inc */

  pthread_mutex_t   tx_control_mutex;
  pthread_cond_t    tx_control_condvar;

  /* Maximum amount of resources which account is allowed to consume. */
  USER_RESOURCES user_resources;

  /*
    Counts resources consumed for this user.
    Use thd_get_user_stats(THD*) rather than USER_CONN::user_stats directly
  */
  USER_STATS user_stats;

} USER_CONN;

typedef struct st_index_stats {
  char name [NAME_LEN + 1];  /* [name] + '\0' */

  /* See variable comments from st_table_stats */
  my_atomic_bigint volatile rows_inserted;
  my_atomic_bigint volatile rows_updated;
  my_atomic_bigint volatile rows_deleted;
  my_atomic_bigint volatile rows_read;
  my_atomic_bigint volatile rows_requested;

  my_atomic_bigint volatile rows_index_first;
  my_atomic_bigint volatile rows_index_next;

  my_io_perf_t io_perf_read;         /* Read IO performance counters */
} INDEX_STATS;

/* 
  Maximum number of indexes for which stats are collected. Tables that have
  more use st_table_stats::indexes[MAX_INDEX_STATS-1] for the extra indexes.
*/
#define MAX_INDEX_STATS 10

typedef struct st_table_stats {
  char db[NAME_LEN + 1];     /* [db] + '\0' */
  char table[NAME_LEN + 1];  /* [table] + '\0' */
  /* Hash table key, table->s->table_cache_key for the table */
  char hash_key[NAME_LEN * 2 + 2];
  int hash_key_len;          /* table->s->key_length for the table */

  INDEX_STATS indexes[MAX_INDEX_STATS];
  uint num_indexes;           /* min(#indexes on table, MAX_INDEX_STATS) */

  volatile my_atomic_bigint keys_dirtied;    /* number of memcache keys dirtied */
  volatile my_atomic_bigint queries_used;    /* number of times used by a query */

  /* TODO(mcallaghan): why are these volatile? */
  volatile my_atomic_bigint rows_inserted;   /* Number of rows inserted */
  volatile my_atomic_bigint rows_updated;    /* Number of rows updated */
  volatile my_atomic_bigint rows_deleted;    /* Number of rows deleted */
  volatile my_atomic_bigint rows_read;       /* Number of rows read for this table */
  volatile my_atomic_bigint rows_requested;  /* Number of row read attempts for
                                        this table.  This counts requests
                                         that do not return a row. */

  comp_stat_t comp_stat;	/* Compression statistics */
  /* See variables of same name in ha_statistics */
  my_atomic_bigint volatile rows_index_first;
  my_atomic_bigint volatile rows_index_next;

  my_io_perf_t io_perf_read;         /* Read IO performance counters */
  my_io_perf_t io_perf_write;        /* Write IO performance counters */
  volatile my_atomic_bigint index_inserts;  /* Number of secondary index inserts. */
  const char* engine_name;
} TABLE_STATS;

/*
   Hack to provide stats for SQL replication slave as THD::user_connect is
   not set for it. See get_user_stats.
*/
extern USER_STATS slave_user_stats;

/*
   Hack to provide stats for anything that doesn't have THD::user_connect except
   the SQL slave.  See get_user_stats.
*/
extern USER_STATS other_user_stats;

/* Resets counters to zero for all users */
extern int
reset_all_user_stats();

/* Intialize an instance of USER_STATS */
extern void
init_user_stats(USER_STATS *user_stats);

/* Update counters at statement end */
void
update_user_stats_after_statement(USER_STATS *us,
                                  THD *thd,
                                  double wall_seconds,
                                  bool is_other_command,
                                  bool is_xid_event,
                                  my_io_perf_t *start_perf_read);

	/* Bits in form->update */
#define REG_MAKE_DUPP		1	/* Make a copy of record when read */
#define REG_NEW_RECORD		2	/* Write a new record if not found */
#define REG_UPDATE		4	/* Uppdate record */
#define REG_DELETE		8	/* Delete found record */
#define REG_PROG		16	/* User is updating database */
#define REG_CLEAR_AFTER_WRITE	32
#define REG_MAY_BE_UPDATED	64
#define REG_AUTO_UPDATE		64	/* Used in D-forms for scroll-tables */
#define REG_OVERWRITE		128
#define REG_SKIP_DUP		256

	/* Bits in form->status */
#define STATUS_NO_RECORD	(1+2)	/* Record isn't usably */
#define STATUS_GARBAGE		1
#define STATUS_NOT_FOUND	2	/* No record in database when needed */
#define STATUS_NO_PARENT	4	/* Parent record wasn't found */
#define STATUS_NOT_READ		8	/* Record isn't read */
#define STATUS_UPDATED		16	/* Record is updated by formula */
#define STATUS_NULL_ROW		32	/* table->null_row is set */
#define STATUS_DELETED		64

/*
  Such interval is "discrete": it is the set of
  { auto_inc_interval_min + k * increment,
    0 <= k <= (auto_inc_interval_values-1) }
  Where "increment" is maintained separately by the user of this class (and is
  currently only thd->variables.auto_increment_increment).
  It mustn't derive from Sql_alloc, because SET INSERT_ID needs to
  allocate memory which must stay allocated for use by the next statement.
*/
class Discrete_interval {
private:
  ulonglong interval_min;
  ulonglong interval_values;
  ulonglong  interval_max;    // excluded bound. Redundant.
public:
  Discrete_interval *next;    // used when linked into Discrete_intervals_list
  void replace(ulonglong start, ulonglong val, ulonglong incr)
  {
    interval_min=    start;
    interval_values= val;
    interval_max=    (val == ULONGLONG_MAX) ? val : start + val * incr;
  }
  Discrete_interval(ulonglong start, ulonglong val, ulonglong incr) :
    next(NULL) { replace(start, val, incr); };
  Discrete_interval() : next(NULL) { replace(0, 0, 0); };
  ulonglong minimum() const { return interval_min;    };
  ulonglong values()  const { return interval_values; };
  ulonglong maximum() const { return interval_max;    };
  /*
    If appending [3,5] to [1,2], we merge both in [1,5] (they should have the
    same increment for that, user of the class has to ensure that). That is
    just a space optimization. Returns 0 if merge succeeded.
  */
  bool merge_if_contiguous(ulonglong start, ulonglong val, ulonglong incr)
  {
    if (interval_max == start)
    {
      if (val == ULONGLONG_MAX)
      {
        interval_values=   interval_max= val;
      }
      else
      {
        interval_values+=  val;
        interval_max=      start + val * incr;
      }
      return 0;
    }
    return 1;
  };
};

/* List of Discrete_interval objects */
class Discrete_intervals_list {
private:
  Discrete_interval        *head;
  Discrete_interval        *tail;
  /*
    When many intervals are provided at the beginning of the execution of a
    statement (in a replication slave or SET INSERT_ID), "current" points to
    the interval being consumed by the thread now (so "current" goes from
    "head" to "tail" then to NULL).
  */
  Discrete_interval        *current;
  uint                  elements; // number of elements
  void set_members(Discrete_interval *h, Discrete_interval *t,
                   Discrete_interval *c, uint el)
  {  
    head= h;
    tail= t;
    current= c;
    elements= el;
  }
  void operator=(Discrete_intervals_list &);  /* prevent use of these */
  Discrete_intervals_list(const Discrete_intervals_list &);

public:
  Discrete_intervals_list() : head(NULL), current(NULL), elements(0) {};
  void empty_no_free()
  {
    set_members(NULL, NULL, NULL, 0);
  }
  void empty()
  {
    for (Discrete_interval *i= head; i;)
    {
      Discrete_interval *next= i->next;
      delete i;
      i= next;
    }
    empty_no_free();
  }
  void copy_shallow(const Discrete_intervals_list * dli)
  {
    head= dli->get_head();
    tail= dli->get_tail();
    current= dli->get_current();
    elements= dli->nb_elements();
  }
  void swap (Discrete_intervals_list * dli)
  {
    Discrete_interval *h, *t, *c;
    uint el;
    h= dli->get_head();
    t= dli->get_tail();
    c= dli->get_current();
    el= dli->nb_elements();
    dli->copy_shallow(this);
    set_members(h, t, c, el);
  }
  const Discrete_interval* get_next()
  {
    Discrete_interval *tmp= current;
    if (current != NULL)
      current= current->next;
    return tmp;
  }
  ~Discrete_intervals_list() { empty(); };
  bool append(ulonglong start, ulonglong val, ulonglong incr);
  bool append(Discrete_interval *interval);
  ulonglong minimum()     const { return (head ? head->minimum() : 0); };
  ulonglong maximum()     const { return (head ? tail->maximum() : 0); };
  uint      nb_elements() const { return elements; }
  Discrete_interval* get_head() const { return head; };
  Discrete_interval* get_tail() const { return tail; };
  Discrete_interval* get_current() const { return current; };
};
