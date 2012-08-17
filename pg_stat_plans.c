/*-------------------------------------------------------------------------
 *
 * pg_stat_plans.c
 *		Track plan execution times across a whole database cluster.
 *
 * Execution costs are totalled for each distinct source plan, and kept in a
 * shared hashtable.	(We track only as many distinct plans as will fit in the
 * designated amount of shared memory.)
 *
 * Normalization is implemented by fingerprinting plans, selectively
 * serializing those fields of each plans's nodes that are judged to be
 * essential to the plan.
 *
 * This jumble is acquired within executor hooks at execution time.
 *
 * Note about locking issues: to create or delete an entry in the shared
 * hashtable, one must hold pgsp->lock exclusively.  Modifying any field
 * in an entry except the counters requires the same.  To look up an entry,
 * one must hold the lock shared.  To read or update the counters within
 * an entry, one must hold the lock shared or exclusive (so the entry doesn't
 * disappear!) and also take the entry's mutex spinlock.
 *
 *
 * Portions Copyright (c) 2012, 2ndQuadrant Ltd.
 * Portions Copyright (c) 2008-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  pg_stat_plans/pg_stat_plans.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/hash.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"


PG_MODULE_MAGIC;

/* Location of stats file */
#define PGSP_DUMP_FILE	"global/pg_stat_plans.stat"

/* This constant defines the magic number in the stats file header */
static const uint32 PGSP_FILE_HEADER = 0x20120328;

/* XXX: Should USAGE_EXEC reflect execution time and/or buffer usage? */
#define USAGE_EXEC(duration)	(1.0)
#define USAGE_INIT				(1.0)	/* including initial planning */
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every entry_dealloc */
#define JUMBLE_SIZE				1024	/* plan jumble size */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */

/*
 * Hashtable key that defines the identity of a hashtable entry.  We separate
 * queries by user and by database even if they are otherwise identical.
 *
 * Presently, the query encoding is fully determined by the source database
 * and so we don't really need it to be in the key.  But that might not always
 * be true. Anyway it's notationally convenient to pass it as part of the key.
 */
typedef struct pgspHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	int			encoding;		/* query encoding */
	uint32		planid;			/* plan identifier */
} pgspHashKey;

/*
 * The actual stats counters kept within pgspEntry.
 */
typedef struct Counters
{
	int64		calls;			/* # of times executed */
	double		total_time;		/* total execution time, in msec */
	int64		rows;			/* total # of retrieved or affected rows */
	int64		shared_blks_hit;	/* # of shared buffer hits */
	int64		shared_blks_read;		/* # of shared disk blocks read */
	int64		shared_blks_written;	/* # of shared disk blocks written */
	int64		local_blks_hit; /* # of local buffer hits */
	int64		local_blks_read;	/* # of local disk blocks read */
	int64		local_blks_written;		/* # of local disk blocks written */
	int64		temp_blks_read; /* # of temp blocks read */
	int64		temp_blks_written;		/* # of temp blocks written */
	double		usage;			/* usage factor */
} Counters;

/*
 * Statistics per plan
 *
 * NB: see the file read/write code before changing field order here.
 */
typedef struct pgspEntry
{
	pgspHashKey key;			/* hash key of entry - MUST BE FIRST */
	Counters	counters;		/* the statistics for this query */
	int			query_len;		/* # of valid bytes in query string */
	slock_t		mutex;			/* protects the counters only */
	char		query[1];		/* VARIABLE LENGTH ARRAY - MUST BE LAST */
	/* Note: the allocated length of query[] is actually pgsp->query_size */
} pgspEntry;

/*
 * Global shared state
 */
typedef struct pgspSharedState
{
	LWLockId	lock;			/* protects hashtable search/modification */
	int			query_size;		/* max query length in bytes */
} pgspSharedState;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct pgspJumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size		jumble_len;
} pgspJumbleState;

/*---- Local variables ----*/

/* Current nesting depth of ExecutorRun calls */
static int	nested_level = 0;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/* Links to shared memory state */
static pgspSharedState *pgsp = NULL;
static HTAB *pgsp_hash = NULL;

/*---- GUC variables ----*/

typedef enum
{
	PGSP_TRACK_NONE,			/* track no plans */
	PGSP_TRACK_TOP,				/* only top level plans */
	PGSP_TRACK_ALL				/* all plans, including nested ones */
}	PGSPTrackLevel;

static const struct config_enum_entry track_options[] =
{
	{"none", PGSP_TRACK_NONE, false},
	{"top", PGSP_TRACK_TOP, false},
	{"all", PGSP_TRACK_ALL, false},
	{NULL, 0, false}
};

static int	pgsp_max;			/* max # plans to track */
static int	pgsp_track;			/* tracking level */
static bool pgsp_save;			/* whether to save stats across shutdown */


#define pgsp_enabled() \
	(pgsp_track == PGSP_TRACK_ALL || \
	(pgsp_track == PGSP_TRACK_TOP && nested_level == 0))

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

Datum		pg_stat_plans_reset(PG_FUNCTION_ARGS);
Datum		pg_stat_plans(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_stat_plans_reset);
PG_FUNCTION_INFO_V1(pg_stat_plans);

static void pgsp_shmem_startup(void);
static void pgsp_shmem_shutdown(int code, Datum arg);
static void pgsp_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgsp_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
				 long count);
static void pgsp_ExecutorFinish(QueryDesc *queryDesc);
static void pgsp_ExecutorEnd(QueryDesc *queryDesc);
static uint32 pgsp_hash_fn(const void *key, Size keysize);
static int	pgsp_match_fn(const void *key1, const void *key2, Size keysize);
static void pgsp_store(const char *query, uint32 planId,
		   double total_time, uint64 rows,
		   const BufferUsage *bufusage);
static Size pgsp_memsize(void);
static pgspEntry *entry_alloc(pgspHashKey *key, const char *query,
			int query_len);
static void entry_dealloc(void);
static void entry_reset(void);
static void AppendJumble(pgspJumbleState *jstate,
			 const unsigned char *item, Size size);
static void JumblePlan(pgspJumbleState *jstate, PlannedStmt *plan);
static void JumbleRangeTable(pgspJumbleState *jstate, List *rtable);
static void JumbleExpr(pgspJumbleState *jstate, Node *node);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_stat_plans functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomIntVariable("pg_stat_plans.max",
	  "Sets the maximum number of plans tracked by pg_stat_plans.",
							NULL,
							&pgsp_max,
							1000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("pg_stat_plans.track",
			   "Selects which plans are tracked by pg_stat_plans.",
							 NULL,
							 &pgsp_track,
							 PGSP_TRACK_TOP,
							 track_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_plans.save",
			   "Save pg_stat_plans statistics across server shutdowns.",
							 NULL,
							 &pgsp_save,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	EmitWarningsOnPlaceholders("pg_stat_plans");

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgsp_shmem_startup().
	 */
	RequestAddinShmemSpace(pgsp_memsize());
	RequestAddinLWLocks(1);

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgsp_shmem_startup;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgsp_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgsp_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgsp_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgsp_ExecutorEnd;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing statistics from file.
 */
static void
pgsp_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;
	FILE	   *file;
	uint32		header;
	int32		num;
	int32		i;
	int			query_size;
	int			buffer_size;
	char	   *buffer = NULL;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgsp = NULL;
	pgsp_hash = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgsp = ShmemInitStruct("pg_stat_plans",
						   sizeof(pgspSharedState),
						   &found);

	if (!found)
	{
		/* First time through ... */
		pgsp->lock = LWLockAssign();
		pgsp->query_size = pgstat_track_activity_query_size;
	}

	/* Be sure everyone agrees on the hash table entry size */
	query_size = pgsp->query_size;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgspHashKey);
	info.entrysize = offsetof(pgspEntry, query) +query_size;
	info.hash = pgsp_hash_fn;
	info.match = pgsp_match_fn;
	pgsp_hash = ShmemInitHash("pg_stat_plans hash",
							  pgsp_max, pgsp_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	/*
	 * If we're in the postmaster (or a standalone backend...), set up a shmem
	 * exit hook to dump the statistics to disk.
	 */
	if (!IsUnderPostmaster)
		on_shmem_exit(pgsp_shmem_shutdown, (Datum) 0);

	/*
	 * Attempt to load old statistics from the dump file, if this is the first
	 * time through and we weren't told not to.
	 */
	if (found || !pgsp_save)
		return;

	/*
	 * Note: we don't bother with locks here, because there should be no other
	 * processes running when this code is reached.
	 */
	file = AllocateFile(PGSP_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
			return;				/* ignore not-found error */
		goto error;
	}

	buffer_size = query_size;
	buffer = (char *) palloc(buffer_size);

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != PGSP_FILE_HEADER ||
		fread(&num, sizeof(int32), 1, file) != 1)
		goto error;

	for (i = 0; i < num; i++)
	{
		pgspEntry	temp;
		pgspEntry  *entry;

		if (fread(&temp, offsetof(pgspEntry, mutex), 1, file) != 1)
			goto error;

		/* Encoding is the only field we can easily sanity-check */
		if (!PG_VALID_BE_ENCODING(temp.key.encoding))
			goto error;

		/* Previous incarnation might have had a larger query_size */
		if (temp.query_len >= buffer_size)
		{
			buffer = (char *) repalloc(buffer, temp.query_len + 1);
			buffer_size = temp.query_len + 1;
		}

		if (fread(buffer, 1, temp.query_len, file) != temp.query_len)
			goto error;
		buffer[temp.query_len] = '\0';

		/* Clip to available length if needed */
		if (temp.query_len >= query_size)
			temp.query_len = pg_encoding_mbcliplen(temp.key.encoding,
												   buffer,
												   temp.query_len,
												   query_size - 1);

		/* make the hashtable entry (discards old entries if too many) */
		entry = entry_alloc(&temp.key, buffer, temp.query_len);

		/* copy in the actual stats */
		entry->counters = temp.counters;
	}

	pfree(buffer);
	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replication slaves,
	 * etc. A new file will be written on next shutdown.
	 */
	unlink(PGSP_DUMP_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_stat_plans file \"%s\": %m",
					PGSP_DUMP_FILE)));
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);
	/* If possible, throw away the bogus file; ignore any error */
	unlink(PGSP_DUMP_FILE);
}

/*
 * shmem_shutdown hook: Dump statistics into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
pgsp_shmem_shutdown(int code, Datum arg)
{
	FILE	   *file;
	HASH_SEQ_STATUS hash_seq;
	int32		num_entries;
	pgspEntry  *entry;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgsp || !pgsp_hash)
		return;

	/* Don't dump if told not to. */
	if (!pgsp_save)
		return;

	file = AllocateFile(PGSP_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGSP_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;
	num_entries = hash_get_num_entries(pgsp_hash);
	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	hash_seq_init(&hash_seq, pgsp_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			len = entry->query_len;

		if (fwrite(entry, offsetof(pgspEntry, mutex), 1, file) != 1 ||
			fwrite(entry->query, 1, len, file) != len)
			goto error;
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file into place, so we atomically replace the old one.
	 */
	if (rename(PGSP_DUMP_FILE ".tmp", PGSP_DUMP_FILE) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename pg_stat_plans file \"%s\": %m",
						PGSP_DUMP_FILE ".tmp")));

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write pg_stat_plans file \"%s\": %m",
					PGSP_DUMP_FILE ".tmp")));
	if (file)
		FreeFile(file);
	unlink(PGSP_DUMP_FILE ".tmp");
}

/*
 * ExecutorStart hook: start up tracking if needed
 */
static void
pgsp_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (pgsp_enabled())
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pgsp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pgsp_ExecutorFinish(QueryDesc *queryDesc)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: store results if needed
 */
static void
pgsp_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && pgsp_enabled())
	{
		pgspJumbleState	jstate;
		int planId;
		/* Set up workspace for plan jumbling */
		jstate.jumble = (unsigned char *) palloc(JUMBLE_SIZE);
		jstate.jumble_len = 0;

		/* Compute plan ID */
		JumblePlan(&jstate, queryDesc->plannedstmt);
		planId = hash_any(jstate.jumble, jstate.jumble_len);

		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		pgsp_store(queryDesc->sourceText,
				   planId,
				   queryDesc->totaltime->total * 1000.0,		/* convert to msec */
				   queryDesc->estate->es_processed,
				   &queryDesc->totaltime->bufusage);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * Calculate hash value for a key
 */
static uint32
pgsp_hash_fn(const void *key, Size keysize)
{
	const pgspHashKey *k = (const pgspHashKey *) key;

	/* we don't bother to include encoding in the hash */
	return hash_uint32((uint32) k->userid) ^
		hash_uint32((uint32) k->dbid) ^
		hash_uint32((uint32) k->planid);
}

/*
 * Compare two keys - zero means match
 */
static int
pgsp_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgspHashKey *k1 = (const pgspHashKey *) key1;
	const pgspHashKey *k2 = (const pgspHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->encoding == k2->encoding &&
		k1->planid == k2->planid)
		return 0;
	else
		return 1;
}

/*
 * Store some statistics for a plans.
 */
static void
pgsp_store(const char *query, uint32 planId,
		   double total_time, uint64 rows,
		   const BufferUsage *bufusage)
{
	pgspHashKey key;
	pgspEntry  *entry;

	Assert(query != NULL);

	/* Safety check... */
	if (!pgsp || !pgsp_hash)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.encoding = GetDatabaseEncoding();
	key.planid = planId;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgsp->lock, LW_SHARED);

	entry = (pgspEntry *) hash_search(pgsp_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		int			query_len;

		/*
		 * We'll need exclusive lock to make a new entry.  There is no point
		 * in holding shared lock while we normalize the string, though.
		 */
		LWLockRelease(pgsp->lock);

		query_len = strlen(query);

		/*
		 * We're just going to store the query string as-is; but we have
		 * to truncate it if over-length.
		 */
		if (query_len >= pgsp->query_size)
			query_len = pg_encoding_mbcliplen(key.encoding,
											  query,
											  query_len,
											  pgsp->query_size - 1);

		/* Acquire exclusive lock as required by entry_alloc() */
		LWLockAcquire(pgsp->lock, LW_EXCLUSIVE);

		entry = entry_alloc(&key, query, query_len);
	}

	/* Increment the counts */
	{
		/*
		 * Grab the spinlock while updating the counters (see comment about
		 * locking rules at the head of the file)
		 */
		volatile pgspEntry *e = (volatile pgspEntry *) entry;

		SpinLockAcquire(&e->mutex);

		e->counters.calls += 1;
		e->counters.total_time += total_time;
		e->counters.rows += rows;
		e->counters.shared_blks_hit += bufusage->shared_blks_hit;
		e->counters.shared_blks_read += bufusage->shared_blks_read;
		e->counters.shared_blks_written += bufusage->shared_blks_written;
		e->counters.local_blks_hit += bufusage->local_blks_hit;
		e->counters.local_blks_read += bufusage->local_blks_read;
		e->counters.local_blks_written += bufusage->local_blks_written;
		e->counters.temp_blks_read += bufusage->temp_blks_read;
		e->counters.temp_blks_written += bufusage->temp_blks_written;
		e->counters.usage += USAGE_EXEC(total_time);

		SpinLockRelease(&e->mutex);
	}

	LWLockRelease(pgsp->lock);
}

/*
 * Reset all plan statistics.
 */
Datum
pg_stat_plans_reset(PG_FUNCTION_ARGS)
{
	if (!pgsp || !pgsp_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_plans must be loaded via shared_preload_libraries")));
	entry_reset();
	PG_RETURN_VOID();
}

#define PG_STAT_PLAN_COLS 14

/*
 * Retrieve plan statistics.
 */
Datum
pg_stat_plans(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Oid			userid = GetUserId();
	bool		is_superuser = superuser();
	HASH_SEQ_STATUS hash_seq;
	pgspEntry  *entry;

	if (!pgsp || !pgsp_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_plans must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(pgsp->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgsp_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[PG_STAT_PLAN_COLS];
		bool		nulls[PG_STAT_PLAN_COLS];
		int			i = 0;
		Counters	tmp;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);

		if (is_superuser || entry->key.userid == userid)
		{
			char	   *qstr;

			qstr = (char *)
				pg_do_encoding_conversion((unsigned char *) entry->query,
										  entry->query_len,
										  entry->key.encoding,
										  GetDatabaseEncoding());
			values[i++] = CStringGetTextDatum(qstr);
			if (qstr != entry->query)
				pfree(qstr);
		}
		else
			values[i++] = CStringGetTextDatum("<insufficient privilege>");

		/* copy counters to a local variable to keep locking time short */
		{
			volatile pgspEntry *e = (volatile pgspEntry *) entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

		values[i++] = Int64GetDatumFast(tmp.calls);
		values[i++] = Float8GetDatumFast(tmp.total_time);
		values[i++] = Int64GetDatumFast(tmp.rows);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_read);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_written);
		values[i++] = Int64GetDatumFast(tmp.local_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.local_blks_read);
		values[i++] = Int64GetDatumFast(tmp.local_blks_written);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_read);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_written);

		Assert(i == PG_STAT_PLAN_COLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgsp->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgsp_memsize(void)
{
	Size		size;
	Size		entrysize;

	size = MAXALIGN(sizeof(pgspSharedState));
	entrysize = offsetof(pgspEntry, query) +pgstat_track_activity_query_size;
	size = add_size(size, hash_estimate_size(pgsp_max, entrysize));

	return size;
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgsp->lock
 *
 * "query" need not be null-terminated; we rely on query_len instead
 *
 * Note: despite needing exclusive lock, it's not an error for the target
 * entry to already exist.	This is because pgsp_store releases and
 * reacquires lock after failing to find a match; so someone else could
 * have made the entry while we waited to get exclusive lock.
 */
static pgspEntry *
entry_alloc(pgspHashKey *key, const char *query, int query_len)
{
	pgspEntry  *entry;
	bool		found;

	/* Make space if needed */
	while (hash_get_num_entries(pgsp_hash) >= pgsp_max)
		entry_dealloc();

	/* Find or create an entry with desired hash code */
	entry = (pgspEntry *) hash_search(pgsp_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(Counters));
		/* set the appropriate initial usage count */
		entry->counters.usage = USAGE_INIT;
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
		/* ... and don't forget the query text */
		Assert(query_len >= 0 && query_len < pgsp->query_size);
		entry->query_len = query_len;
		memcpy(entry->query, query, query_len);
		entry->query[query_len] = '\0';
	}

	return entry;
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgspEntry *const *) lhs)->counters.usage;
	double		r_usage = (*(pgspEntry *const *) rhs)->counters.usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exclusive lock on pgsp->lock.
 */
static void
entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgspEntry **entries;
	pgspEntry  *entry;
	int			nvictims;
	int			i;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */

	entries = palloc(hash_get_num_entries(pgsp_hash) * sizeof(pgspEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgsp_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->counters.usage *= USAGE_DECREASE_FACTOR;
	}

	qsort(entries, i, sizeof(pgspEntry *), entry_cmp);

	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgsp_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}

/*
 * Release all entries.
 */
static void
entry_reset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgspEntry  *entry;

	LWLockAcquire(pgsp->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgsp_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgsp_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgsp->lock);
}

/*
 * AppendJumble: Append a value that is substantive in a given query to
 * the current jumble.
 */
static void
AppendJumble(pgspJumbleState *jstate, const unsigned char *item, Size size)
{
	unsigned char *jumble = jstate->jumble;
	Size		jumble_len = jstate->jumble_len;

	/*
	 * Whenever the jumble buffer is full, we hash the current contents and
	 * reset the buffer to contain just that hash value, thus relying on the
	 * hash to summarize everything so far.
	 */
	while (size > 0)
	{
		Size		part_size;

		if (jumble_len >= JUMBLE_SIZE)
		{
			uint32		start_hash = hash_any(jumble, JUMBLE_SIZE);

			memcpy(jumble, &start_hash, sizeof(start_hash));
			jumble_len = sizeof(start_hash);
		}
		part_size = Min(size, JUMBLE_SIZE - jumble_len);
		memcpy(jumble + jumble_len, item, part_size);
		jumble_len += part_size;
		item += part_size;
		size -= part_size;
	}
	jstate->jumble_len = jumble_len;
}

/*
 * Wrappers around AppendJumble to encapsulate details of serialization
 * of individual local variable elements.
 */
#define APP_JUMB(item) \
	AppendJumble(jstate, (const unsigned char *) &(item), sizeof(item))
#define APP_JUMB_STRING(str) \
	AppendJumble(jstate, (const unsigned char *) (str), strlen(str) + 1)

/*
 * JumblePlan: Selectively serialize the plan, appending significant
 * data to the "query jumble" while ignoring nonsignificant data.
 */
static void
JumblePlan(pgspJumbleState *jstate, PlannedStmt *plan)
{
	Assert(IsA(plan, PlannedStmt));

	APP_JUMB(plan->commandType);
	/* resultRelation is usually predictable from commandType */
	JumbleExpr(jstate, (Node *) plan->planTree);
	JumbleRangeTable(jstate, plan->rtable);
	JumbleExpr(jstate, (Node *) plan->resultRelations);
	JumbleExpr(jstate, (Node *) plan->utilityStmt);
	JumbleExpr(jstate, (Node *) plan->intoClause);
	JumbleExpr(jstate, (Node *) plan->subplans);
	JumbleExpr(jstate, (Node *) plan->rewindPlanIDs);
	JumbleExpr(jstate, (Node *) plan->rowMarks);
	JumbleExpr(jstate, (Node *) plan->relationOids);
	JumbleExpr(jstate, (Node *) plan->invalItems);
}

/*
 * Jumble a range table
 */
static void
JumbleRangeTable(pgspJumbleState *jstate, List *rtable)
{
	ListCell   *lc;

	foreach(lc, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		Assert(IsA(rte, RangeTblEntry));
		APP_JUMB(rte->rtekind);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				APP_JUMB(rte->relid);
				break;
			case RTE_SUBQUERY:
				Assert(!rte->subquery);
				break;
			case RTE_JOIN:
				APP_JUMB(rte->jointype);
				break;
			case RTE_FUNCTION:
				JumbleExpr(jstate, rte->funcexpr);
				break;
			case RTE_VALUES:
				JumbleExpr(jstate, (Node *) rte->values_lists);
				break;
			case RTE_CTE:

				/*
				 * Depending on the CTE name here isn't ideal, but it's the
				 * only info we have to identify the referenced WITH item.
				 */
				APP_JUMB_STRING(rte->ctename);
				APP_JUMB(rte->ctelevelsup);
				break;
			default:
				elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
				break;
		}
	}
}

/*
 * Jumble a plan tree
 *
 * In general this function should handle all the same node types that
 * expression_tree_walker() does, and therefore it's coded to be as parallel
 * to that function as possible.  However, since we are only invoked on
 * queries immediately post-parse-analysis, we need not handle node types
 * that only appear in planning.
 *
 * Note: the reason we don't simply use expression_tree_walker() is that the
 * point of that function is to support tree walkers that don't care about
 * most tree node types, but here we care about all types.	We should complain
 * about any unrecognized node type.
 */
static void
JumbleExpr(pgspJumbleState *jstate, Node *node)
{
	ListCell   *temp;

	if (node == NULL)
		return;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/*
	 * We always emit the node's NodeTag, then any additional fields that are
	 * considered significant, and then we recurse to any child nodes.
	 */
	APP_JUMB(node->type);

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				APP_JUMB(var->varno);
				APP_JUMB(var->varattno);
				APP_JUMB(var->varlevelsup);
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/* We jumble only the constant's type, not its value */
				APP_JUMB(c->consttype);
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				APP_JUMB(p->paramkind);
				APP_JUMB(p->paramid);
				APP_JUMB(p->paramtype);
			}
			break;
		case T_Aggref:
			{
				Aggref	   *expr = (Aggref *) node;

				APP_JUMB(expr->aggfnoid);
				JumbleExpr(jstate, (Node *) expr->args);
				JumbleExpr(jstate, (Node *) expr->aggorder);
				JumbleExpr(jstate, (Node *) expr->aggdistinct);
			}
			break;
		case T_WindowFunc:
			{
				WindowFunc *expr = (WindowFunc *) node;

				APP_JUMB(expr->winfnoid);
				APP_JUMB(expr->winref);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;

				JumbleExpr(jstate, (Node *) aref->refupperindexpr);
				JumbleExpr(jstate, (Node *) aref->reflowerindexpr);
				JumbleExpr(jstate, (Node *) aref->refexpr);
				JumbleExpr(jstate, (Node *) aref->refassgnexpr);
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;

				APP_JUMB(expr->funcid);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_NamedArgExpr:
			{
				NamedArgExpr *nae = (NamedArgExpr *) node;

				APP_JUMB(nae->argnumber);
				JumbleExpr(jstate, (Node *) nae->arg);
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				APP_JUMB(expr->opno);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

				APP_JUMB(expr->opno);
				APP_JUMB(expr->useOr);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				APP_JUMB(expr->boolop);
				JumbleExpr(jstate, (Node *) expr->args);
			}
			break;
		case T_SubLink:
			{
				SubLink    *sublink = (SubLink *) node;

				APP_JUMB(sublink->subLinkType);
				JumbleExpr(jstate, (Node *) sublink->testexpr);
				JumblePlan(jstate, (PlannedStmt*) sublink->subselect);
			}
			break;
		case T_FieldSelect:
			{
				FieldSelect *fs = (FieldSelect *) node;

				APP_JUMB(fs->fieldnum);
				JumbleExpr(jstate, (Node *) fs->arg);
			}
			break;
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;

				JumbleExpr(jstate, (Node *) fstore->arg);
				JumbleExpr(jstate, (Node *) fstore->newvals);
			}
			break;
		case T_RelabelType:
			{
				RelabelType *rt = (RelabelType *) node;

				APP_JUMB(rt->resulttype);
				JumbleExpr(jstate, (Node *) rt->arg);
			}
			break;
		case T_CoerceViaIO:
			{
				CoerceViaIO *cio = (CoerceViaIO *) node;

				APP_JUMB(cio->resulttype);
				JumbleExpr(jstate, (Node *) cio->arg);
			}
			break;
		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acexpr = (ArrayCoerceExpr *) node;

				APP_JUMB(acexpr->resulttype);
				JumbleExpr(jstate, (Node *) acexpr->arg);
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *crexpr = (ConvertRowtypeExpr *) node;

				APP_JUMB(crexpr->resulttype);
				JumbleExpr(jstate, (Node *) crexpr->arg);
			}
			break;
		case T_CollateExpr:
			{
				CollateExpr *ce = (CollateExpr *) node;

				APP_JUMB(ce->collOid);
				JumbleExpr(jstate, (Node *) ce->arg);
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *caseexpr = (CaseExpr *) node;

				JumbleExpr(jstate, (Node *) caseexpr->arg);
				foreach(temp, caseexpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(temp);

					Assert(IsA(when, CaseWhen));
					JumbleExpr(jstate, (Node *) when->expr);
					JumbleExpr(jstate, (Node *) when->result);
				}
				JumbleExpr(jstate, (Node *) caseexpr->defresult);
			}
			break;
		case T_CaseTestExpr:
			{
				CaseTestExpr *ct = (CaseTestExpr *) node;

				APP_JUMB(ct->typeId);
			}
			break;
		case T_ArrayExpr:
			JumbleExpr(jstate, (Node *) ((ArrayExpr *) node)->elements);
			break;
		case T_RowExpr:
			JumbleExpr(jstate, (Node *) ((RowExpr *) node)->args);
			break;
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;

				APP_JUMB(rcexpr->rctype);
				JumbleExpr(jstate, (Node *) rcexpr->largs);
				JumbleExpr(jstate, (Node *) rcexpr->rargs);
			}
			break;
		case T_CoalesceExpr:
			JumbleExpr(jstate, (Node *) ((CoalesceExpr *) node)->args);
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *mmexpr = (MinMaxExpr *) node;

				APP_JUMB(mmexpr->op);
				JumbleExpr(jstate, (Node *) mmexpr->args);
			}
			break;
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;

				APP_JUMB(xexpr->op);
				JumbleExpr(jstate, (Node *) xexpr->named_args);
				JumbleExpr(jstate, (Node *) xexpr->args);
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				APP_JUMB(nt->nulltesttype);
				JumbleExpr(jstate, (Node *) nt->arg);
			}
			break;
		case T_BooleanTest:
			{
				BooleanTest *bt = (BooleanTest *) node;

				APP_JUMB(bt->booltesttype);
				JumbleExpr(jstate, (Node *) bt->arg);
			}
			break;
		case T_CoerceToDomain:
			{
				CoerceToDomain *cd = (CoerceToDomain *) node;

				APP_JUMB(cd->resulttype);
				JumbleExpr(jstate, (Node *) cd->arg);
			}
			break;
		case T_CoerceToDomainValue:
			{
				CoerceToDomainValue *cdv = (CoerceToDomainValue *) node;

				APP_JUMB(cdv->typeId);
			}
			break;
		case T_SetToDefault:
			{
				SetToDefault *sd = (SetToDefault *) node;

				APP_JUMB(sd->typeId);
			}
			break;
		case T_CurrentOfExpr:
			{
				CurrentOfExpr *ce = (CurrentOfExpr *) node;

				APP_JUMB(ce->cvarno);
				if (ce->cursor_name)
					APP_JUMB_STRING(ce->cursor_name);
				APP_JUMB(ce->cursor_param);
			}
			break;
		case T_TargetEntry:
			{
				TargetEntry *tle = (TargetEntry *) node;

				APP_JUMB(tle->resno);
				APP_JUMB(tle->ressortgroupref);
				JumbleExpr(jstate, (Node *) tle->expr);
			}
			break;
		case T_RangeTblRef:
			{
				RangeTblRef *rtr = (RangeTblRef *) node;

				APP_JUMB(rtr->rtindex);
			}
			break;
		case T_JoinExpr:
			{
				JoinExpr   *join = (JoinExpr *) node;

				APP_JUMB(join->jointype);
				APP_JUMB(join->isNatural);
				APP_JUMB(join->rtindex);
				JumbleExpr(jstate, join->larg);
				JumbleExpr(jstate, join->rarg);
				JumbleExpr(jstate, join->quals);
			}
			break;
		case T_FromExpr:
			{
				FromExpr   *from = (FromExpr *) node;

				JumbleExpr(jstate, (Node *) from->fromlist);
				JumbleExpr(jstate, from->quals);
			}
			break;
		case T_IntoClause:
			{
				IntoClause *into = (IntoClause *) node;

				JumbleExpr(jstate, (Node *) into->colNames);
				JumbleExpr(jstate, (Node *) into->options);
			}
			break;
		case T_List:
			foreach(temp, (List *) node)
			{
				JumbleExpr(jstate, (Node *) lfirst(temp));
			}
			break;
		case T_IntList:
		case T_OidList:
			foreach(temp, (List *) node)
			{
				int val = (int)lfirst(temp);
				APP_JUMB(val);
			}
			break;
		case T_SortGroupClause:
			{
				SortGroupClause *sgc = (SortGroupClause *) node;

				APP_JUMB(sgc->tleSortGroupRef);
				APP_JUMB(sgc->eqop);
				APP_JUMB(sgc->sortop);
				APP_JUMB(sgc->nulls_first);
			}
			break;
		case T_WindowClause:
			{
				WindowClause *wc = (WindowClause *) node;

				APP_JUMB(wc->winref);
				APP_JUMB(wc->frameOptions);
				JumbleExpr(jstate, (Node *) wc->partitionClause);
				JumbleExpr(jstate, (Node *) wc->orderClause);
				JumbleExpr(jstate, wc->startOffset);
				JumbleExpr(jstate, wc->endOffset);
			}
			break;
		case T_CommonTableExpr:
			{
				CommonTableExpr *cte = (CommonTableExpr *) node;

				/* we store the string name because RTE_CTE RTEs need it */
				APP_JUMB_STRING(cte->ctename);
				JumblePlan(jstate, (PlannedStmt*) cte->ctequery);
			}
			break;
		case T_SetOperationStmt:
			{
				SetOperationStmt *setop = (SetOperationStmt *) node;

				APP_JUMB(setop->op);
				APP_JUMB(setop->all);
				JumbleExpr(jstate, setop->larg);
				JumbleExpr(jstate, setop->rarg);
			}
			break;
		/* Plan nodes: */
		case T_Result:
			{
				Result *res = (Result*) node;
			}
			break;
		case T_ModifyTable:
			{
				ModifyTable *mt = (ModifyTable *) node;
			}
			break;
		case T_Append:
			{
				Append *app = (Append *) node;
			}
			break;
		case T_MergeAppend:
			{
				MergeAppend *ma = (MergeAppend *) node;
			}
			break;
		case T_RecursiveUnion:
			{
				RecursiveUnion *ru = (RecursiveUnion *) node;
			}
			break;
		case T_BitmapAnd:
			{
				BitmapAnd *ba = (BitmapAnd *) node;
			}
			break;
		case T_BitmapOr:
			{
				BitmapOr *bo = (BitmapOr *) node;
			}
			break;
		case T_Scan:
			{
				Scan *sc = (Scan *) node;
			}
			break;
		case T_SeqScan:
			{
				SeqScan *sqs = (SeqScan *) node;
			}
			break;
		case T_IndexScan:
			{
				IndexScan *is = (IndexScan *) node;
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bis = (BitmapIndexScan *) node;
			}
			break;
		case T_BitmapHeapScan:
			{
				BitmapHeapScan *bhs = (BitmapHeapScan *) node;
			}
			break;
		case T_TidScan:
			{
				TidScan *tsc = (TidScan *) node;
			}
			break;
		case T_SubqueryScan:
			{
				SubqueryScan *sqs = (SubqueryScan *) node;
			}
			break;
		case T_FunctionScan:
			{
				FunctionScan *fs = (FunctionScan *) node;
			}
			break;
		case T_ValuesScan:
			{
				ValuesScan *vs = (ValuesScan *) node;
			}
			break;
		case T_CteScan:
			{
				CteScan *ctesc = (CteScan *) node;
			}
			break;
		case T_WorkTableScan:
			{
				WorkTableScan *wts = (WorkTableScan *) node;
			}
			break;
		case T_ForeignScan:
			{
				ForeignScan *fs = (ForeignScan *) node;
			}
			break;
		case T_FdwPlan:
			{
				/* TODO: Something. No such struct. */
			}
			break;
		case T_Join:
			{
				Join *j = (Join *) node;
			}
			break;
		case T_NestLoop:
			{
				NestLoop *nl = (NestLoop *) node;
			}
			break;
		case T_MergeJoin:
			{
				MergeJoin *mj = (MergeJoin *) node;
			}
			break;
		case T_HashJoin:
			{
				HashJoin *hj = (HashJoin *) node;
			}
			break;
		case T_Material:
			{
				Material *ma = (Material *) node;
			}
			break;
		case T_Sort:
			{
				Sort *so = (Sort *) node;
			}
			break;
		case T_Group:
			{
				Group *gr = (Group *) node;
			}
			break;
		case T_Agg:
			{
				Agg *ag = (Agg *) node;
			}
			break;
		case T_WindowAgg:
			{
				WindowAgg *wa = (WindowAgg *) node;
			}
			break;
		case T_Unique:
			{
				Unique *un = (Unique *) node;
			}
			break;
		case T_Hash:
			{
				Hash *hash = (Hash *) node;
			}
			break;
		case T_SetOp:
			{
				SetOp *so = (SetOp *) node;
			}
			break;
		case T_LockRows:
			{
				LockRows *lr = (LockRows *) node;
			}
			break;
		case T_Limit:
			{
				Limit *lim = (Limit *) node;
			}
			break;
		/* these aren't subclasses of Plan: */
		case T_NestLoopParam:
			{
				NestLoopParam *nlp = (NestLoopParam *) node;
			}
			break;
		case T_PlanRowMark:
			{
				PlanRowMark *prm = (PlanRowMark*) node;
			}
			break;
		case T_PlanInvalItem:
			{
				PlanInvalItem *pii = (PlanInvalItem*) node;
			}
			break;
		/* Non-plan nodes that are known to appear in plannedStmts: */
		case T_Integer:
		case T_Float:
		case T_String:
		case T_BitString:
		case T_Null:
			{
				Value *v = (Value *) node;
			}
			break;
		case T_ColumnDef:
			{
				ColumnDef *cd = (ColumnDef *) node;
			}
			break;
		case T_DefElem:
			{
				DefElem *de = (DefElem *) node;
			}
			break;
		default:
			/* Only a warning, since we can stumble along anyway */
			elog(WARNING, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
}
