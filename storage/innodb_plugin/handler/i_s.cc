/*****************************************************************************

Copyright (c) 2007, 2009, Innobase Oy. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/**************************************************//**
@file handler/i_s.cc
InnoDB INFORMATION SCHEMA tables interface to MySQL.

Created July 18, 2007 Vasil Dimov
*******************************************************/

#include <mysql_priv.h>
#include <mysqld_error.h>

#include <m_ctype.h>
#include <hash.h>
#include <myisampack.h>
#include <mysys_err.h>
#include <my_sys.h>
#include "i_s.h"
#include <mysql/plugin.h>

extern "C" {
#include "trx0i_s.h"
#include "trx0trx.h" /* for TRX_QUE_STATE_STR_MAX_LEN */
#include "buf0buddy.h" /* for i_s_cmpmem */
#include "buf0buf.h" /* for buf_pool and PAGE_ZIP_MIN_SIZE */
#include "buf0lru.h" /* for buf_pool and PAGE_ZIP_MIN_SIZE */
#include "ha_prototypes.h" /* for innobase_convert_name() */
#include "srv0start.h" /* for srv_was_started */
}

static const char plugin_author[] = "Innobase Oy";

#define OK(expr)		\
	if ((expr) != 0) {	\
		DBUG_RETURN(1);	\
	}

#define RETURN_IF_INNODB_NOT_STARTED(plugin_name)			\
do {									\
	if (!srv_was_started) {						\
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN,	\
				    ER_CANT_FIND_SYSTEM_REC,		\
				    "InnoDB: SELECTing from "		\
				    "INFORMATION_SCHEMA.%s but "	\
				    "the InnoDB storage engine "	\
				    "is not installed", plugin_name);	\
		DBUG_RETURN(0);						\
	}								\
} while (0)

#if !defined __STRICT_ANSI__ && defined __GNUC__ && (__GNUC__) > 2 && !defined __INTEL_COMPILER
#define STRUCT_FLD(name, value)	name: value
#else
#define STRUCT_FLD(name, value)	value
#endif

/* Don't use a static const variable here, as some C++ compilers (notably
HPUX aCC: HP ANSI C++ B3910B A.03.65) can't handle it. */
#define END_OF_ST_FIELD_INFO \
	{STRUCT_FLD(field_name,		NULL), \
	 STRUCT_FLD(field_length,	0), \
	 STRUCT_FLD(field_type,		MYSQL_TYPE_NULL), \
	 STRUCT_FLD(value,		0), \
	 STRUCT_FLD(field_flags,	0), \
	 STRUCT_FLD(old_name,		""), \
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)}

/*
Use the following types mapping:

C type	ST_FIELD_INFO::field_type
---------------------------------
long			MYSQL_TYPE_LONGLONG
(field_length=MY_INT64_NUM_DECIMAL_DIGITS)

long unsigned		MYSQL_TYPE_LONGLONG
(field_length=MY_INT64_NUM_DECIMAL_DIGITS, field_flags=MY_I_S_UNSIGNED)

char*			MYSQL_TYPE_STRING
(field_length=n)

float			MYSQL_TYPE_FLOAT
(field_length=0 is ignored)

void*			MYSQL_TYPE_LONGLONG
(field_length=MY_INT64_NUM_DECIMAL_DIGITS, field_flags=MY_I_S_UNSIGNED)

boolean (if else)	MYSQL_TYPE_LONG
(field_length=1)

time_t			MYSQL_TYPE_DATETIME
(field_length=0 ignored)
---------------------------------
*/

/* XXX these are defined in mysql_priv.h inside #ifdef MYSQL_SERVER */
bool schema_table_store_record(THD *thd, TABLE *table);
void localtime_to_TIME(MYSQL_TIME *to, struct tm *from);
bool check_global_access(THD *thd, ulong want_access);

/*******************************************************************//**
Common function to fill any of the dynamic tables:
INFORMATION_SCHEMA.innodb_trx
INFORMATION_SCHEMA.innodb_locks
INFORMATION_SCHEMA.innodb_lock_waits
@return	0 on success */
static
int
trx_i_s_common_fill_table(
/*======================*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond);	/*!< in: condition (not used) */

/*******************************************************************//**
Unbind a dynamic INFORMATION_SCHEMA table.
@return	0 on success */
static
int
i_s_common_deinit(
/*==============*/
	void*	p);	/*!< in/out: table schema object */

/*******************************************************************//**
Auxiliary function to store time_t value in MYSQL_TYPE_DATETIME
field.
@return	0 on success */
static
int
field_store_time_t(
/*===============*/
	Field*	field,	/*!< in/out: target field for storage */
	time_t	time)	/*!< in: value to store */
{
	MYSQL_TIME	my_time;
	struct tm	tm_time;

#if 0
	/* use this if you are sure that `variables' and `time_zone'
	are always initialized */
	thd->variables.time_zone->gmt_sec_to_TIME(
		&my_time, (my_time_t) time);
#else
	localtime_r(&time, &tm_time);
	localtime_to_TIME(&my_time, &tm_time);
	my_time.time_type = MYSQL_TIMESTAMP_DATETIME;
#endif

	return(field->store_time(&my_time, MYSQL_TIMESTAMP_DATETIME));
}

/*******************************************************************//**
Auxiliary function to store char* value in MYSQL_TYPE_STRING field.
@return	0 on success */
static
int
field_store_string(
/*===============*/
	Field*		field,	/*!< in/out: target field for storage */
	const char*	str)	/*!< in: NUL-terminated utf-8 string,
				or NULL */
{
	int	ret;

	if (str != NULL) {

		ret = field->store(str, strlen(str),
				   system_charset_info);
		field->set_notnull();
	} else {

		ret = 0; /* success */
		field->set_null();
	}

	return(ret);
}

/*******************************************************************//**
Auxiliary function to store ulint value in MYSQL_TYPE_LONGLONG field.
If the value is ULINT_UNDEFINED then the field it set to NULL.
@return	0 on success */
static
int
field_store_ulint(
/*==============*/
	Field*	field,	/*!< in/out: target field for storage */
	ulint	n)	/*!< in: value to store */
{
	int	ret;

	if (n != ULINT_UNDEFINED) {

		ret = field->store(n);
		field->set_notnull();
	} else {

		ret = 0; /* success */
		field->set_null();
	}

	return(ret);
}

/* Fields of the dynamic table INFORMATION_SCHEMA.innodb_trx */
static ST_FIELD_INFO	innodb_trx_fields_info[] =
{
#define IDX_TRX_ID		0
	{STRUCT_FLD(field_name,		"trx_id"),
	 STRUCT_FLD(field_length,	TRX_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_STATE		1
	{STRUCT_FLD(field_name,		"trx_state"),
	 STRUCT_FLD(field_length,	TRX_QUE_STATE_STR_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_STARTED		2
	{STRUCT_FLD(field_name,		"trx_started"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_DATETIME),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_REQUESTED_LOCK_ID	3
	{STRUCT_FLD(field_name,		"trx_requested_lock_id"),
	 STRUCT_FLD(field_length,	TRX_I_S_LOCK_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_WAIT_STARTED	4
	{STRUCT_FLD(field_name,		"trx_wait_started"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_DATETIME),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_WEIGHT		5
	{STRUCT_FLD(field_name,		"trx_weight"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_MYSQL_THREAD_ID	6
	{STRUCT_FLD(field_name,		"trx_mysql_thread_id"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_TRX_QUERY		7
	{STRUCT_FLD(field_name,		"trx_query"),
	 STRUCT_FLD(field_length,	TRX_I_S_TRX_QUERY_MAX_LEN),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};

/*******************************************************************//**
Read data from cache buffer and fill the INFORMATION_SCHEMA.innodb_trx
table with it.
@return	0 on success */
static
int
fill_innodb_trx_from_cache(
/*=======================*/
	trx_i_s_cache_t*	cache,	/*!< in: cache to read from */
	THD*			thd,	/*!< in: used to call
					schema_table_store_record() */
	TABLE*			table)	/*!< in/out: fill this table */
{
	Field**	fields;
	ulint	rows_num;
	char	lock_id[TRX_I_S_LOCK_ID_MAX_LEN + 1];
	ulint	i;

	DBUG_ENTER("fill_innodb_trx_from_cache");

	fields = table->field;

	rows_num = trx_i_s_cache_get_rows_used(cache,
					       I_S_INNODB_TRX);

	for (i = 0; i < rows_num; i++) {

		i_s_trx_row_t*	row;
		char		trx_id[TRX_ID_MAX_LEN + 1];

		row = (i_s_trx_row_t*)
			trx_i_s_cache_get_nth_row(
				cache, I_S_INNODB_TRX, i);

		/* trx_id */
		ut_snprintf(trx_id, sizeof(trx_id), TRX_ID_FMT, row->trx_id);
		OK(field_store_string(fields[IDX_TRX_ID], trx_id));

		/* trx_state */
		OK(field_store_string(fields[IDX_TRX_STATE],
				      row->trx_state));

		/* trx_started */
		OK(field_store_time_t(fields[IDX_TRX_STARTED],
				      (time_t) row->trx_started));

		/* trx_requested_lock_id */
		/* trx_wait_started */
		if (row->trx_wait_started != 0) {

			OK(field_store_string(
				   fields[IDX_TRX_REQUESTED_LOCK_ID],
				   trx_i_s_create_lock_id(
					   row->requested_lock_row,
					   lock_id, sizeof(lock_id))));
			/* field_store_string() sets it no notnull */

			OK(field_store_time_t(
				   fields[IDX_TRX_WAIT_STARTED],
				   (time_t) row->trx_wait_started));
			fields[IDX_TRX_WAIT_STARTED]->set_notnull();
		} else {

			fields[IDX_TRX_REQUESTED_LOCK_ID]->set_null();
			fields[IDX_TRX_WAIT_STARTED]->set_null();
		}

		/* trx_weight */
		OK(fields[IDX_TRX_WEIGHT]->store((longlong) row->trx_weight,
						 true));

		/* trx_mysql_thread_id */
		OK(fields[IDX_TRX_MYSQL_THREAD_ID]->store(
			   row->trx_mysql_thread_id));

		/* trx_query */
		OK(field_store_string(fields[IDX_TRX_QUERY],
				      row->trx_query));

		OK(schema_table_store_record(thd, table));
	}

	DBUG_RETURN(0);
}

/*******************************************************************//**
Bind the dynamic table INFORMATION_SCHEMA.innodb_trx
@return	0 on success */
static
int
innodb_trx_init(
/*============*/
	void*	p)	/*!< in/out: table schema object */
{
	ST_SCHEMA_TABLE*	schema;

	DBUG_ENTER("innodb_trx_init");

	schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = innodb_trx_fields_info;
	schema->fill_table = trx_i_s_common_fill_table;

	DBUG_RETURN(0);
}

static struct st_mysql_information_schema	i_s_info =
{
	MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION
};

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_trx =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_TRX"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "InnoDB transactions"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, innodb_trx_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

/* Fields of the dynamic table INFORMATION_SCHEMA.innodb_locks */
static ST_FIELD_INFO	innodb_locks_fields_info[] =
{
#define IDX_LOCK_ID		0
	{STRUCT_FLD(field_name,		"lock_id"),
	 STRUCT_FLD(field_length,	TRX_I_S_LOCK_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_TRX_ID		1
	{STRUCT_FLD(field_name,		"lock_trx_id"),
	 STRUCT_FLD(field_length,	TRX_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_MODE		2
	{STRUCT_FLD(field_name,		"lock_mode"),
	 /* S[,GAP] X[,GAP] IS[,GAP] IX[,GAP] AUTO_INC UNKNOWN */
	 STRUCT_FLD(field_length,	32),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_TYPE		3
	{STRUCT_FLD(field_name,		"lock_type"),
	 STRUCT_FLD(field_length,	32 /* RECORD|TABLE|UNKNOWN */),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_TABLE		4
	{STRUCT_FLD(field_name,		"lock_table"),
	 STRUCT_FLD(field_length,	1024),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_INDEX		5
	{STRUCT_FLD(field_name,		"lock_index"),
	 STRUCT_FLD(field_length,	1024),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_SPACE		6
	{STRUCT_FLD(field_name,		"lock_space"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED | MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_PAGE		7
	{STRUCT_FLD(field_name,		"lock_page"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED | MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_REC		8
	{STRUCT_FLD(field_name,		"lock_rec"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED | MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_LOCK_DATA		9
	{STRUCT_FLD(field_name,		"lock_data"),
	 STRUCT_FLD(field_length,	TRX_I_S_LOCK_DATA_MAX_LEN),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_MAYBE_NULL),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};

/*******************************************************************//**
Read data from cache buffer and fill the INFORMATION_SCHEMA.innodb_locks
table with it.
@return	0 on success */
static
int
fill_innodb_locks_from_cache(
/*=========================*/
	trx_i_s_cache_t*	cache,	/*!< in: cache to read from */
	THD*			thd,	/*!< in: MySQL client connection */
	TABLE*			table)	/*!< in/out: fill this table */
{
	Field**	fields;
	ulint	rows_num;
	char	lock_id[TRX_I_S_LOCK_ID_MAX_LEN + 1];
	ulint	i;

	DBUG_ENTER("fill_innodb_locks_from_cache");

	fields = table->field;

	rows_num = trx_i_s_cache_get_rows_used(cache,
					       I_S_INNODB_LOCKS);

	for (i = 0; i < rows_num; i++) {

		i_s_locks_row_t*	row;

		/* note that the decoded database or table name is
		never expected to be longer than NAME_LEN;
		NAME_LEN for database name
		2 for surrounding quotes around database name
		NAME_LEN for table name
		2 for surrounding quotes around table name
		1 for the separating dot (.)
		9 for the #mysql50# prefix */
		char			buf[2 * NAME_LEN + 14];
		const char*		bufend;

		char			lock_trx_id[TRX_ID_MAX_LEN + 1];

		row = (i_s_locks_row_t*)
			trx_i_s_cache_get_nth_row(
				cache, I_S_INNODB_LOCKS, i);

		/* lock_id */
		trx_i_s_create_lock_id(row, lock_id, sizeof(lock_id));
		OK(field_store_string(fields[IDX_LOCK_ID],
				      lock_id));

		/* lock_trx_id */
		ut_snprintf(lock_trx_id, sizeof(lock_trx_id),
			    TRX_ID_FMT, row->lock_trx_id);
		OK(field_store_string(fields[IDX_LOCK_TRX_ID], lock_trx_id));

		/* lock_mode */
		OK(field_store_string(fields[IDX_LOCK_MODE],
				      row->lock_mode));

		/* lock_type */
		OK(field_store_string(fields[IDX_LOCK_TYPE],
				      row->lock_type));

		/* lock_table */
		bufend = innobase_convert_name(buf, sizeof(buf),
					       row->lock_table,
					       strlen(row->lock_table),
					       thd, TRUE);
		OK(fields[IDX_LOCK_TABLE]->store(buf, bufend - buf,
						 system_charset_info));

		/* lock_index */
		if (row->lock_index != NULL) {

			bufend = innobase_convert_name(buf, sizeof(buf),
						       row->lock_index,
						       strlen(row->lock_index),
						       thd, FALSE);
			OK(fields[IDX_LOCK_INDEX]->store(buf, bufend - buf,
							 system_charset_info));
			fields[IDX_LOCK_INDEX]->set_notnull();
		} else {

			fields[IDX_LOCK_INDEX]->set_null();
		}

		/* lock_space */
		OK(field_store_ulint(fields[IDX_LOCK_SPACE],
				     row->lock_space));

		/* lock_page */
		OK(field_store_ulint(fields[IDX_LOCK_PAGE],
				     row->lock_page));

		/* lock_rec */
		OK(field_store_ulint(fields[IDX_LOCK_REC],
				     row->lock_rec));

		/* lock_data */
		OK(field_store_string(fields[IDX_LOCK_DATA],
				      row->lock_data));

		OK(schema_table_store_record(thd, table));
	}

	DBUG_RETURN(0);
}

/*******************************************************************//**
Bind the dynamic table INFORMATION_SCHEMA.innodb_locks
@return	0 on success */
static
int
innodb_locks_init(
/*==============*/
	void*	p)	/*!< in/out: table schema object */
{
	ST_SCHEMA_TABLE*	schema;

	DBUG_ENTER("innodb_locks_init");

	schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = innodb_locks_fields_info;
	schema->fill_table = trx_i_s_common_fill_table;

	DBUG_RETURN(0);
}

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_locks =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_LOCKS"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "InnoDB conflicting locks"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, innodb_locks_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

/* Fields of the dynamic table INFORMATION_SCHEMA.innodb_lock_waits */
static ST_FIELD_INFO	innodb_lock_waits_fields_info[] =
{
#define IDX_REQUESTING_TRX_ID	0
	{STRUCT_FLD(field_name,		"requesting_trx_id"),
	 STRUCT_FLD(field_length,	TRX_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_REQUESTED_LOCK_ID	1
	{STRUCT_FLD(field_name,		"requested_lock_id"),
	 STRUCT_FLD(field_length,	TRX_I_S_LOCK_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_BLOCKING_TRX_ID	2
	{STRUCT_FLD(field_name,		"blocking_trx_id"),
	 STRUCT_FLD(field_length,	TRX_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

#define IDX_BLOCKING_LOCK_ID	3
	{STRUCT_FLD(field_name,		"blocking_lock_id"),
	 STRUCT_FLD(field_length,	TRX_I_S_LOCK_ID_MAX_LEN + 1),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};

/*******************************************************************//**
Read data from cache buffer and fill the
INFORMATION_SCHEMA.innodb_lock_waits table with it.
@return	0 on success */
static
int
fill_innodb_lock_waits_from_cache(
/*==============================*/
	trx_i_s_cache_t*	cache,	/*!< in: cache to read from */
	THD*			thd,	/*!< in: used to call
					schema_table_store_record() */
	TABLE*			table)	/*!< in/out: fill this table */
{
	Field**	fields;
	ulint	rows_num;
	char	requested_lock_id[TRX_I_S_LOCK_ID_MAX_LEN + 1];
	char	blocking_lock_id[TRX_I_S_LOCK_ID_MAX_LEN + 1];
	ulint	i;

	DBUG_ENTER("fill_innodb_lock_waits_from_cache");

	fields = table->field;

	rows_num = trx_i_s_cache_get_rows_used(cache,
					       I_S_INNODB_LOCK_WAITS);

	for (i = 0; i < rows_num; i++) {

		i_s_lock_waits_row_t*	row;

		char	requesting_trx_id[TRX_ID_MAX_LEN + 1];
		char	blocking_trx_id[TRX_ID_MAX_LEN + 1];

		row = (i_s_lock_waits_row_t*)
			trx_i_s_cache_get_nth_row(
				cache, I_S_INNODB_LOCK_WAITS, i);

		/* requesting_trx_id */
		ut_snprintf(requesting_trx_id, sizeof(requesting_trx_id),
			    TRX_ID_FMT, row->requested_lock_row->lock_trx_id);
		OK(field_store_string(fields[IDX_REQUESTING_TRX_ID],
				      requesting_trx_id));

		/* requested_lock_id */
		OK(field_store_string(
			   fields[IDX_REQUESTED_LOCK_ID],
			   trx_i_s_create_lock_id(
				   row->requested_lock_row,
				   requested_lock_id,
				   sizeof(requested_lock_id))));

		/* blocking_trx_id */
		ut_snprintf(blocking_trx_id, sizeof(blocking_trx_id),
			    TRX_ID_FMT, row->blocking_lock_row->lock_trx_id);
		OK(field_store_string(fields[IDX_BLOCKING_TRX_ID],
				      blocking_trx_id));

		/* blocking_lock_id */
		OK(field_store_string(
			   fields[IDX_BLOCKING_LOCK_ID],
			   trx_i_s_create_lock_id(
				   row->blocking_lock_row,
				   blocking_lock_id,
				   sizeof(blocking_lock_id))));

		OK(schema_table_store_record(thd, table));
	}

	DBUG_RETURN(0);
}

/*******************************************************************//**
Bind the dynamic table INFORMATION_SCHEMA.innodb_lock_waits
@return	0 on success */
static
int
innodb_lock_waits_init(
/*===================*/
	void*	p)	/*!< in/out: table schema object */
{
	ST_SCHEMA_TABLE*	schema;

	DBUG_ENTER("innodb_lock_waits_init");

	schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = innodb_lock_waits_fields_info;
	schema->fill_table = trx_i_s_common_fill_table;

	DBUG_RETURN(0);
}

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_lock_waits =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_LOCK_WAITS"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, "Innobase Oy"),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "InnoDB which lock is blocking which"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, innodb_lock_waits_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

/*******************************************************************//**
Common function to fill any of the dynamic tables:
INFORMATION_SCHEMA.innodb_trx
INFORMATION_SCHEMA.innodb_locks
INFORMATION_SCHEMA.innodb_lock_waits
@return	0 on success */
static
int
trx_i_s_common_fill_table(
/*======================*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond)	/*!< in: condition (not used) */
{
	const char*		table_name;
	int			ret;
	trx_i_s_cache_t*	cache;

	DBUG_ENTER("trx_i_s_common_fill_table");

	/* deny access to non-superusers */
	if (check_global_access(thd, PROCESS_ACL)) {

		DBUG_RETURN(0);
	}

	/* minimize the number of places where global variables are
	referenced */
	cache = trx_i_s_cache;

	/* which table we have to fill? */
	table_name = tables->schema_table_name;
	/* or table_name = tables->schema_table->table_name; */

	RETURN_IF_INNODB_NOT_STARTED(table_name);

	/* update the cache */
	trx_i_s_cache_start_write(cache);
	trx_i_s_possibly_fetch_data_into_cache(cache);
	trx_i_s_cache_end_write(cache);

	if (trx_i_s_cache_is_truncated(cache)) {

		/* XXX show warning to user if possible */
		fprintf(stderr, "Warning: data in %s truncated due to "
			"memory limit of %d bytes\n", table_name,
			TRX_I_S_MEM_LIMIT);
	}

	ret = 0;

	trx_i_s_cache_start_read(cache);

	if (innobase_strcasecmp(table_name, "innodb_trx") == 0) {

		if (fill_innodb_trx_from_cache(
			cache, thd, tables->table) != 0) {

			ret = 1;
		}

	} else if (innobase_strcasecmp(table_name, "innodb_locks") == 0) {

		if (fill_innodb_locks_from_cache(
			cache, thd, tables->table) != 0) {

			ret = 1;
		}

	} else if (innobase_strcasecmp(table_name, "innodb_lock_waits") == 0) {

		if (fill_innodb_lock_waits_from_cache(
			cache, thd, tables->table) != 0) {

			ret = 1;
		}

	} else {

		/* huh! what happened!? */
		fprintf(stderr,
			"InnoDB: trx_i_s_common_fill_table() was "
			"called to fill unknown table: %s.\n"
			"This function only knows how to fill "
			"innodb_trx, innodb_locks and "
			"innodb_lock_waits tables.\n", table_name);

		ret = 1;
	}

	trx_i_s_cache_end_read(cache);

#if 0
	DBUG_RETURN(ret);
#else
	/* if this function returns something else than 0 then a
	deadlock occurs between the mysqld server and mysql client,
	see http://bugs.mysql.com/29900 ; when that bug is resolved
	we can enable the DBUG_RETURN(ret) above */
	ret++;  // silence a gcc46 warning
	DBUG_RETURN(0);
#endif
}

/* Fields of the dynamic table information_schema.innodb_cmp. */
static ST_FIELD_INFO	i_s_cmp_fields_info[] =
{
	{STRUCT_FLD(field_name,		"page_size"),
	 STRUCT_FLD(field_length,	5),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Compressed Page Size"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_ops"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Compressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_ops_ok"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of"
					" Successful Compressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Compressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_ok_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Successful Compressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_primary_ops"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Compressions in primary indexes"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_primary_ops_ok"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of"
					" Successful Compressions in primary indexes"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_primary_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Primary Index Compressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_primary_ok_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Successful Primary Index "
		    "Compressions, in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_secondary_ops"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Secondary Index Compressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_secondary_ops_ok"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of"
					" Successful Secondary Index Compressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_secondary_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Secondary Index Compressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"compress_secondary_ok_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Successful Secondary Index "
		    "Compressions, in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"uncompress_ops"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Decompressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"uncompress_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Decompressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"uncompress_primary_ops"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Primary Index Decompressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"uncompress_primary_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Primary Index Decompressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"uncompress_secondary_ops"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Secondary Index Decompressions"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"uncompress_secondary_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Secondary Index Decompressions,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"page_splits_comp_fail"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Page Splits Caused by Compression"
	      " Failures"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};


/*******************************************************************//**
Fill the dynamic table information_schema.innodb_cmp or
innodb_cmp_reset.
@return	0 on success, 1 on failure */
static
int
i_s_cmp_fill_low(
/*=============*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond,	/*!< in: condition (ignored) */
	ibool		reset)	/*!< in: TRUE=reset cumulated counts */
{
	TABLE*	table	= (TABLE *) tables->table;
	int	status	= 0;

	DBUG_ENTER("i_s_cmp_fill_low");

	/* deny access to non-superusers */
	if (check_global_access(thd, PROCESS_ACL)) {

		DBUG_RETURN(0);
	}

	RETURN_IF_INNODB_NOT_STARTED(tables->schema_table_name);

	for (uint i = 0; i < PAGE_ZIP_NUM_SSIZE - 1; i++) {
		page_zip_stat_t*	zip_stat = &page_zip_stat[i];
		int col = -1;
		table->field[++col]->store(PAGE_ZIP_MIN_SIZE << i);

		/* The cumulated counts are not protected by any
		mutex.  Thus, some operation in page0zip.c could
		increment a counter between the time we read it and
		clear it.  We could introduce mutex protection, but it
		could cause a measureable performance hit in
		page0zip.c. */
		table->field[++col]->store(zip_stat->compressed);
		table->field[++col]->store(zip_stat->compressed_ok);
		table->field[++col]->store(
			(ulong) (zip_stat->compressed_usec / 1000000));
		table->field[++col]->store(
			(ulong) (zip_stat->compressed_ok_usec / 1000000));
		table->field[++col]->store(zip_stat->compressed_primary);
		table->field[++col]->store(zip_stat->compressed_primary_ok);
		table->field[++col]->store(
			(ulong) (zip_stat->compressed_primary_usec / 1000000));
		table->field[++col]->store(
			(ulong) (zip_stat->compressed_primary_ok_usec / 1000000));
		table->field[++col]->store(zip_stat->compressed_secondary);
		table->field[++col]->store(zip_stat->compressed_secondary_ok);
		table->field[++col]->store(
			(ulong) (zip_stat->compressed_secondary_usec / 1000000));
		table->field[++col]->store(
			(ulong) (zip_stat->compressed_secondary_ok_usec / 1000000));
		table->field[++col]->store(zip_stat->decompressed);
		table->field[++col]->store(
			(ulong) (zip_stat->decompressed_usec / 1000000));
		table->field[++col]->store(zip_stat->decompressed_primary);
		table->field[++col]->store(
			(ulong) (zip_stat->decompressed_primary_usec / 1000000));
		table->field[++col]->store(zip_stat->decompressed_secondary);
		table->field[++col]->store(
			(ulong) (zip_stat->decompressed_secondary_usec / 1000000));
		table->field[++col]->store(zip_stat->page_splits_comp_fail);

		if (reset) {
			memset(zip_stat, 0, sizeof *zip_stat);
		}

		if (schema_table_store_record(thd, table)) {
			status = 1;
			break;
		}
	}

	DBUG_RETURN(status);
}

/*******************************************************************//**
Fill the dynamic table information_schema.innodb_cmp.
@return	0 on success, 1 on failure */
static
int
i_s_cmp_fill(
/*=========*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond)	/*!< in: condition (ignored) */
{
	return(i_s_cmp_fill_low(thd, tables, cond, FALSE));
}

/*******************************************************************//**
Fill the dynamic table information_schema.innodb_cmp_reset.
@return	0 on success, 1 on failure */
static
int
i_s_cmp_reset_fill(
/*===============*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond)	/*!< in: condition (ignored) */
{
	return(i_s_cmp_fill_low(thd, tables, cond, TRUE));
}

/*******************************************************************//**
Bind the dynamic table information_schema.innodb_cmp.
@return	0 on success */
static
int
i_s_cmp_init(
/*=========*/
	void*	p)	/*!< in/out: table schema object */
{
	DBUG_ENTER("i_s_cmp_init");
	ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = i_s_cmp_fields_info;
	schema->fill_table = i_s_cmp_fill;

	DBUG_RETURN(0);
}

/*******************************************************************//**
Bind the dynamic table information_schema.innodb_cmp_reset.
@return	0 on success */
static
int
i_s_cmp_reset_init(
/*===============*/
	void*	p)	/*!< in/out: table schema object */
{
	DBUG_ENTER("i_s_cmp_reset_init");
	ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = i_s_cmp_fields_info;
	schema->fill_table = i_s_cmp_reset_fill;

	DBUG_RETURN(0);
}

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_cmp =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_CMP"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "Statistics for the InnoDB compression"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, i_s_cmp_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_cmp_reset =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_CMP_RESET"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "Statistics for the InnoDB compression;"
		   " reset cumulated counts"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, i_s_cmp_reset_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

/* Fields of the dynamic table information_schema.innodb_cmpmem. */
static ST_FIELD_INFO	i_s_cmpmem_fields_info[] =
{
	{STRUCT_FLD(field_name,		"page_size"),
	 STRUCT_FLD(field_length,	5),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Buddy Block Size"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"pages_used"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Currently in Use"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"pages_free"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Currently Available"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"relocation_ops"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Number of Relocations"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"relocation_time"),
	 STRUCT_FLD(field_length,	MY_INT32_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Total Duration of Relocations,"
		    " in Seconds"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};

/*******************************************************************//**
Fill the dynamic table information_schema.innodb_cmpmem or
innodb_cmpmem_reset.
@return	0 on success, 1 on failure */
static
int
i_s_cmpmem_fill_low(
/*================*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond,	/*!< in: condition (ignored) */
	ibool		reset)	/*!< in: TRUE=reset cumulated counts */
{
	TABLE*	table	= (TABLE *) tables->table;
	int	status	= 0;

	DBUG_ENTER("i_s_cmpmem_fill_low");

	/* deny access to non-superusers */
	if (check_global_access(thd, PROCESS_ACL)) {

		DBUG_RETURN(0);
	}

	RETURN_IF_INNODB_NOT_STARTED(tables->schema_table_name);

	buf_pool_mutex_enter();

	for (uint x = 0; x <= BUF_BUDDY_SIZES; x++) {
		buf_buddy_stat_t*	buddy_stat = &buf_buddy_stat[x];

		table->field[0]->store(BUF_BUDDY_LOW << x);
		table->field[1]->store(buddy_stat->used);
		table->field[2]->store(UNIV_LIKELY(x < BUF_BUDDY_SIZES)
				       ? UT_LIST_GET_LEN(buf_pool->zip_free[x])
				       : 0);
		table->field[3]->store((longlong) buddy_stat->relocated, true);
		table->field[4]->store(
			(ulong) (buddy_stat->relocated_usec / 1000000));

		if (reset) {
			/* This is protected by buf_pool_mutex. */
			buddy_stat->relocated = 0;
			buddy_stat->relocated_usec = 0;
		}

		if (schema_table_store_record(thd, table)) {
			status = 1;
			break;
		}
	}

	buf_pool_mutex_exit();
	DBUG_RETURN(status);
}

/*******************************************************************//**
Fill the dynamic table information_schema.innodb_cmpmem.
@return	0 on success, 1 on failure */
static
int
i_s_cmpmem_fill(
/*============*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond)	/*!< in: condition (ignored) */
{
	return(i_s_cmpmem_fill_low(thd, tables, cond, FALSE));
}

/*******************************************************************//**
Fill the dynamic table information_schema.innodb_cmpmem_reset.
@return	0 on success, 1 on failure */
static
int
i_s_cmpmem_reset_fill(
/*==================*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond)	/*!< in: condition (ignored) */
{
	return(i_s_cmpmem_fill_low(thd, tables, cond, TRUE));
}

/*******************************************************************//**
Bind the dynamic table information_schema.innodb_cmpmem.
@return	0 on success */
static
int
i_s_cmpmem_init(
/*============*/
	void*	p)	/*!< in/out: table schema object */
{
	DBUG_ENTER("i_s_cmpmem_init");
	ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = i_s_cmpmem_fields_info;
	schema->fill_table = i_s_cmpmem_fill;

	DBUG_RETURN(0);
}

/*******************************************************************//**
Bind the dynamic table information_schema.innodb_cmpmem_reset.
@return	0 on success */
static
int
i_s_cmpmem_reset_init(
/*==================*/
	void*	p)	/*!< in/out: table schema object */
{
	DBUG_ENTER("i_s_cmpmem_reset_init");
	ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = i_s_cmpmem_fields_info;
	schema->fill_table = i_s_cmpmem_reset_fill;

	DBUG_RETURN(0);
}

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_cmpmem =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_CMPMEM"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "Statistics for the InnoDB compressed buffer pool"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, i_s_cmpmem_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_cmpmem_reset =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_CMPMEM_RESET"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "Statistics for the InnoDB compressed buffer pool;"
		   " reset cumulated counts"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, i_s_cmpmem_reset_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};

/* Fields of the dynamic table information_schema.innodb_file_status. */
static ST_FIELD_INFO i_s_file_status_fields_info[]=
{
	{STRUCT_FLD(field_name,		"FILE"),
	 STRUCT_FLD(field_length,	FN_REFLEN),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"File"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"OPERATION"),
	 STRUCT_FLD(field_length,	FN_REFLEN),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Operation"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"REQUESTS"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		"Requests"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"OLD"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		"Old"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"BYTES"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		"Bytes"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"BYTES/R"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_FLOAT),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Bytes/r"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"SVC:SECS"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_FLOAT),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Svc:secs"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"SVC:MSECS/R"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_FLOAT),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Svc:msecs/r"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"SVC:MAX_MSECS"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		"Svc:max_msecs"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"WAIT:SECS"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_FLOAT),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Wait:secs"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"WAIT:MSECS/R"),
	 STRUCT_FLD(field_length,	0),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_FLOAT),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		"Wait:msecs/r"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	{STRUCT_FLD(field_name,		"WAIT:MAX_MSECS"),
	 STRUCT_FLD(field_length,	MY_INT64_NUM_DECIMAL_DIGITS),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_LONGLONG),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	MY_I_S_UNSIGNED),
	 STRUCT_FLD(old_name,		"Wait:max_msecs"),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};

/************************************************************************//**
Fill in a single row for the dynamic table
information_schema.innodb_file_status.
@return	0 on success */
static
int
i_s_file_status_fill_low(
/*=====================*/
	THD*		thd,	  /*!< in: thread */
	TABLE*	        table,	  /*!< in/out: table to fill */
	const char*	file_name,/*!< in: file name for this record */
	const char*	operation,/*!< in: either "read" or "write" */
	my_io_perf_t*	io_perf)  /*!< in: stats collected for the table */
{
	DBUG_ENTER("i_s_file_status_fill_low");

        double nzero_requests = max(io_perf->requests, 1);

        OK(field_store_string(table->field[0], file_name));
        OK(field_store_string(table->field[1], operation));
        OK(table->field[2]->store(io_perf->requests));
        OK(table->field[3]->store(io_perf->old_ios));
        OK(table->field[4]->store(io_perf->bytes));
        OK(table->field[5]->store((double) io_perf->bytes /
				  nzero_requests));
        OK(table->field[6]->store(io_perf->svc_usecs / 1000000.0));
        OK(table->field[7]->store((io_perf->svc_usecs / 1000.0) /
				  nzero_requests));
        OK(table->field[8]->store(io_perf->svc_usecs_max / 1000));
        OK(table->field[9]->store(io_perf->wait_usecs / 1000000.0));
        OK(table->field[10]->store((io_perf->wait_usecs / 1000.0) /
				   nzero_requests));
        OK(table->field[11]->store(io_perf->wait_usecs_max / 1000));

	OK(schema_table_store_record(thd, table));

	DBUG_RETURN(0);
}

/************************************************************************//**
Populates the FILE_STATUS information_schema table.
@return	0 on success */
static
int
i_s_file_status_fill(
/*=================*/
	THD*		thd,	/*!< in: thread */
	TABLE_LIST*	tables,	/*!< in/out: tables to fill */
	COND*		cond)	/*!< in: condition (not used) */
{
	TABLE *table = tables->table;
        fil_system_t*	system	= fil_system;
        fil_space_t*	space;

	DBUG_ENTER("i_s_file_status");

	/* deny access to non-superusers */
	if (check_global_access(thd, PROCESS_ACL)) {

		DBUG_RETURN(0);
	}

	RETURN_IF_INNODB_NOT_STARTED(tables->schema_table_name);

        mutex_enter(&(system->mutex));

	space = UT_LIST_GET_FIRST(system->space_list);

	while (space != NULL) {
		if (space->io_perf2.read.requests) {
			if (i_s_file_status_fill_low(thd, table, space->name, "read",
							&(space->io_perf2.read)))
			{
				mutex_exit(&(system->mutex));
				DBUG_RETURN(1);
			}
		}
		if (space->io_perf2.write.requests) {
			if (i_s_file_status_fill_low(thd, table, space->name, "write",
							&(space->io_perf2.write)))
			{
				mutex_exit(&(system->mutex));
				DBUG_RETURN(1);
			}
		}
		space = UT_LIST_GET_NEXT(space_list, space);
	}

	mutex_exit(&(system->mutex));

	DBUG_RETURN(0);
}

/*******************************************************************//**
Bind the dynamic table information_schema.innodb_file_status.
@return	0 on success */
static
int
i_s_file_status_init(
/*=========*/
	void*	p)	/*!< in/out: table schema object */
{
	DBUG_ENTER("i_s_file_status_init");
	ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = i_s_file_status_fields_info;
	schema->fill_table = i_s_file_status_fill;

	DBUG_RETURN(0);
}

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_file_status =
{
	/* the plugin type (a MYSQL_XXX_PLUGIN value) */
	/* int */
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

	/* pointer to type-specific plugin descriptor */
	/* void* */
	STRUCT_FLD(info, &i_s_info),

	/* plugin name */
	/* const char* */
	STRUCT_FLD(name, "INNODB_FILE_STATUS"),

	/* plugin author (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(author, plugin_author),

	/* general descriptive text (for SHOW PLUGINS) */
	/* const char* */
	STRUCT_FLD(descr, "Per-file statistics for the InnoDB file IO"),

	/* the plugin license (PLUGIN_LICENSE_XXX) */
	/* int */
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

	/* the function to invoke when plugin is loaded */
	/* int (*)(void*); */
	STRUCT_FLD(init, i_s_file_status_init),

	/* the function to invoke when plugin is unloaded */
	/* int (*)(void*); */
	STRUCT_FLD(deinit, i_s_common_deinit),

	/* plugin version (for SHOW PLUGINS) */
	/* unsigned int */
	STRUCT_FLD(version, INNODB_VERSION_SHORT),

	/* struct st_mysql_show_var* */
	STRUCT_FLD(status_vars, NULL),

	/* struct st_mysql_sys_var** */
	STRUCT_FLD(system_vars, NULL),

	/* reserved for dependency checking */
	/* void* */
	STRUCT_FLD(__reserved1, NULL)
};


/***********************************************************************
*/
static ST_FIELD_INFO	i_s_innodb_admin_command_info[] =
{
	{STRUCT_FLD(field_name,		"result_message"),
	 STRUCT_FLD(field_length,	1024),
	 STRUCT_FLD(field_type,		MYSQL_TYPE_STRING),
	 STRUCT_FLD(value,		0),
	 STRUCT_FLD(field_flags,	0),
	 STRUCT_FLD(old_name,		""),
	 STRUCT_FLD(open_method,	SKIP_OPEN_TABLE)},

	END_OF_ST_FIELD_INFO
};

#ifndef INNODB_COMPATIBILITY_HOOKS
#error InnoDB needs MySQL to be built with #define INNODB_COMPATIBILITY_HOOKS
#endif

extern "C" {
char **thd_query(MYSQL_THD thd);
}

static
int
i_s_innodb_admin_command_fill(
/*==========================*/
	THD*		thd,
	TABLE_LIST*	tables,
	COND*		cond)
{
	TABLE*	i_s_table	= (TABLE *) tables->table;
	char**	query_str;
	char*	ptr;
	char	quote	= '\0';
	const char*	command_head = "XTRA_";

	DBUG_ENTER("i_s_innodb_admin_command_fill");

	/* deny access to non-superusers */
	if (check_global_access(thd, PROCESS_ACL)) {
		DBUG_RETURN(0);
	}

	if(thd_sql_command(thd) != SQLCOM_SELECT) {
		field_store_string(i_s_table->field[0],
			"SELECT command is only accepted.");
		goto end_func;
	}

	query_str = thd_query(thd);
	ptr = *query_str;

	for (; *ptr; ptr++) {
		if (*ptr == quote) {
			quote = '\0';
		} else if (quote) {
		} else if (*ptr == '`' || *ptr == '"') {
			quote = *ptr;
		} else {
			long	i;
			for (i = 0; command_head[i]; i++) {
				if (toupper((int)(unsigned char)(ptr[i]))
				    != toupper((int)(unsigned char)
				      (command_head[i]))) {
					goto nomatch;
				}
			}
			break;
nomatch:
			;
		}
	}

	if (!*ptr) {
		field_store_string(i_s_table->field[0],
			"No XTRA_* command in the SQL statement."
			" Please add /*!XTRA_xxxx*/ to the SQL.");
		goto end_func;
	}

	if (!strncasecmp("XTRA_HELLO", ptr, 10)) {
		/* This is example command XTRA_HELLO */

		ut_print_timestamp(stderr);
		fprintf(stderr, " InnoDB: administration command test for XtraDB"
				" 'XTRA_HELLO' was detected.\n");

		field_store_string(i_s_table->field[0],
			"Hello!");
		goto end_func;
	}
	else if (!strncasecmp("XTRA_LRU_DUMP", ptr, 13)) {
		ut_print_timestamp(stderr);
		fprintf(stderr, " InnoDB: administration command 'XTRA_LRU_DUMP'"
				" was detected.\n");

		if (buf_LRU_file_dump()) {
			field_store_string(i_s_table->field[0],
				"XTRA_LRU_DUMP was succeeded.");
		} else {
			field_store_string(i_s_table->field[0],
				"XTRA_LRU_DUMP was failed.");
		}

		goto end_func;
	}
	else if (!strncasecmp("XTRA_LRU_RESTORE", ptr, 16)) {
		ut_print_timestamp(stderr);
		fprintf(stderr, " InnoDB: administration command 'XTRA_LRU_RESTORE'"
				" was detected.\n");

		if (buf_LRU_file_restore()) {
			field_store_string(i_s_table->field[0],
				"XTRA_LRU_RESTORE was succeeded.");
		} else {
			field_store_string(i_s_table->field[0],
				"XTRA_LRU_RESTORE was failed.");
		}

		goto end_func;
	}

	field_store_string(i_s_table->field[0],
		"Undefined XTRA_* command.");
	goto end_func;

end_func:
	if (schema_table_store_record(thd, i_s_table)) {
		DBUG_RETURN(1);
	} else {
		DBUG_RETURN(0);
	}
}

static
int
i_s_innodb_admin_command_init(
/*==========================*/
	void*	p)
{
	DBUG_ENTER("i_s_innodb_admin_command_init");
	ST_SCHEMA_TABLE* schema = (ST_SCHEMA_TABLE*) p;

	schema->fields_info = i_s_innodb_admin_command_info;
	schema->fill_table = i_s_innodb_admin_command_fill;

	DBUG_RETURN(0);
}

UNIV_INTERN struct st_mysql_plugin	i_s_innodb_admin_command =
{
	STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),
	STRUCT_FLD(info, &i_s_info),
	STRUCT_FLD(name, "XTRADB_ADMIN_COMMAND"),
	STRUCT_FLD(author, plugin_author),
	STRUCT_FLD(descr, "XtraDB specific command acceptor"),
	STRUCT_FLD(license, PLUGIN_LICENSE_GPL),
	STRUCT_FLD(init, i_s_innodb_admin_command_init),
	STRUCT_FLD(deinit, i_s_common_deinit),
	STRUCT_FLD(version, 0x0100 /* 1.0 */),
	STRUCT_FLD(status_vars, NULL),
	STRUCT_FLD(system_vars, NULL),
	STRUCT_FLD(__reserved1, NULL)
};

/*******************************************************************//**
Unbind a dynamic INFORMATION_SCHEMA table.
@return	0 on success */
static
int
i_s_common_deinit(
/*==============*/
	void*	p)	/*!< in/out: table schema object */
{
	DBUG_ENTER("i_s_common_deinit");

	/* Do nothing */

	DBUG_RETURN(0);
}
