#ifndef LONGLONG_MAX
#define LONGLONG_MAX 10000000000000ll
#endif

long long	treestore_max_mem_use = 0;
long long	treestore_current_mem_use = 0;
long long	treestore_journal_lower_max = 0;
long long	treestore_journal_upper_max = 0;
long long	treestore_journal_size = 0;
long long	treestore_cleanup_time = 500;
/// a leak
char* 		treestore_contact_points = new char[400];
char* 		treestore_red_address = new char[400];
/// if there are more threads than this active (in locked state)
/// new transactions are throttled to reduce concurrency
long long	treestore_max_thread_concurrency = 7;

double		treestore_column_cache_factor = 0.1;
my_bool		treestore_efficient_text = FALSE;

char		treestore_reduce_tree_use_on_unlock = FALSE;
char		treestore_reduce_index_tree_use_on_unlock = FALSE;
char		treestore_reduce_storage_use_on_unlock = TRUE;
char		treestore_use_primitive_indexes = TRUE;
char		treestore_block_read_ahead = FALSE;
char		treestore_resolve_values_from_index = FALSE;
char		treestore_use_internal_pool = FALSE;
char		treestore_reset_statistics = FALSE;

static MYSQL_SYSVAR_LONGLONG(journal_lower_max, treestore_journal_lower_max,
  PLUGIN_VAR_RQCMDARG,
  "The size the journal reaches before the journal export is attempted",
  NULL, NULL, 1*1024*1024*1024LL, 256*1024*1024LL, LONGLONG_MAX, 1024*1024LL);

static MYSQL_SYSVAR_LONGLONG(journal_upper_max, treestore_journal_upper_max,
  PLUGIN_VAR_RQCMDARG,
  "The size the journal reaches before all new transactional locking is aborted",
  NULL, NULL, 16LL*1024LL*1024LL*1024LL, 256*1024*1024LL, LONGLONG_MAX, 1024*1024LL);

static MYSQL_SYSVAR_LONGLONG(journal_size, treestore_journal_size,
  PLUGIN_VAR_RQCMDARG|PLUGIN_VAR_READONLY,
  "The current journal size",
  NULL, NULL, 0LL, 0LL, LONGLONG_MAX, 1024LL);

static MYSQL_SYSVAR_LONGLONG(cleanup_time, treestore_cleanup_time,
  PLUGIN_VAR_RQCMDARG|PLUGIN_VAR_READONLY,
  "The memory cleanup time in millis",
  NULL, NULL, 3600LL, 1LL, LONGLONG_MAX, 500LL);

static MYSQL_SYSVAR_LONGLONG(max_mem_use, treestore_max_mem_use,
  PLUGIN_VAR_RQCMDARG,
  "The ammount of memory used before treestore starts flushing caches etc.",
  NULL, NULL, 1024*1024*1024LL, 32*1024*1024LL, LONGLONG_MAX, 1024*1024L);


static MYSQL_SYSVAR_LONGLONG(current_mem_use, treestore_current_mem_use,
  PLUGIN_VAR_RQCMDARG|PLUGIN_VAR_READONLY,
  "The current ammount of memory used by treestore",
  NULL, NULL, 128*1024*1024LL, 256*1024*1024LL, LONGLONG_MAX, 1024*1024L);

static MYSQL_SYSVAR_BOOL(efficient_text, treestore_efficient_text,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Uses more efficient in memory text, varchar and varbinary storage"
  "Default is false",
  NULL, NULL, FALSE);

static MYSQL_SYSVAR_DOUBLE(column_cache_factor, treestore_column_cache_factor,
  PLUGIN_VAR_RQCMDARG,
  "The size the journal reaches before the journal export is attempted",
  NULL, NULL, 0.5, 0.01, 0.99, 0.4);

static MYSQL_SYSVAR_BOOL(reduce_tree_use_on_unlock, treestore_reduce_tree_use_on_unlock,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "enables or disables reducing tree use on unlock - causes reader transactions to release locks and rollback too"
  "Default is false (no reducing)",
  NULL, NULL, FALSE);

static MYSQL_SYSVAR_BOOL(reduce_storage_use_on_unlock, treestore_reduce_storage_use_on_unlock,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "enables or disables reducing storage use on unlock"
  "Default is TRUE ",
  NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(reduce_index_tree_use_on_unlock, treestore_reduce_index_tree_use_on_unlock,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "enables or disables reducing index tree use on unlock -inneffective without treestore_reduce_tree_use_on_unlock "
  "Default is false (no reducing)",
  NULL, NULL, FALSE);

static MYSQL_SYSVAR_BOOL(use_primitive_indexes, treestore_use_primitive_indexes,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "enables or disables primitive index trees - primitive index trees use less memory"
  "Default is true",
  NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(block_read_ahead, treestore_block_read_ahead,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "read ahead block storage until block memory is exhausted"
  "Default is false",
  NULL, NULL, FALSE);


static MYSQL_SYSVAR_STR(contact_points,treestore_contact_points,
	PLUGIN_VAR_RQCMDARG,
	"contact point list for cassandra storage",
	NULL, NULL, "");

static MYSQL_SYSVAR_STR(red_address,treestore_red_address,
	PLUGIN_VAR_RQCMDARG,
	"nonde name and address must be unique and acessible from outside the machine",
	NULL, NULL, "localhost");

static MYSQL_SYSVAR_BOOL(resolve_values_from_index, treestore_resolve_values_from_index,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "resolve values from index first before consulting columns"
  "Default is true",
  NULL, NULL, TRUE);

static MYSQL_SYSVAR_BOOL(use_internal_pool, treestore_use_internal_pool,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "use the internal pool to improve concurrency"
  "Default is false",
  NULL, NULL, FALSE);

static MYSQL_SYSVAR_BOOL(reset_statistics, treestore_reset_statistics,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "reset all statistics"
  "Default is false",
  NULL, NULL, FALSE);

