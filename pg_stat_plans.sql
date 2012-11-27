--
-- pg_stat_plans.sql
--

-- Originally from http://blog.ioguix.net/
CREATE OR REPLACE FUNCTION normalize_query(IN TEXT, OUT TEXT) AS $body$
  SELECT
	regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(

    $1,

    -- Remove extra space, new line and tab caracters by a single space
    '\s+',                          ' ',           'g'   ),

    -- Remove string content
    $$\\'$$,                        '',            'g'   ),
    $$'[^']*'$$,                    $$?$$,        'g'   ),
    $$''('')+$$,                    $$?$$,        'g'   ),

    -- Remove NULL parameters
    '=\s*NULL',                     '=?',          'g'   ),

    -- Remove numbers
    '([^a-z_$-])-?([0-9]+)',        '\1'||'?',     'g'   ),

    -- Remove hexadecimal numbers
    '([^a-z_$-])0x[0-9a-f]{1,10}',  '\1'||'?',    'g'   ),

    -- Remove IN values
    'in\s*\([''0x,\s]*\)',          'in (...)',    'g'   )
  ;
$body$
STRICT IMMUTABLE LANGUAGE SQL;

-- Register functions.
CREATE FUNCTION pg_stat_plans_reset()
RETURNS void
AS '$libdir/pg_stat_plans'
LANGUAGE C;

CREATE FUNCTION pg_stat_plans(
    OUT userid oid,
    OUT dbid oid,
    OUT planid oid,
    OUT query text,
    OUT had_our_search_path boolean,
    OUT from_our_database boolean,
    OUT query_valid boolean,
    OUT calls int8,
    OUT total_time float8,
    OUT rows int8,
    OUT shared_blks_hit int8,
    OUT shared_blks_read int8,
    OUT shared_blks_written int8,
    OUT local_blks_hit int8,
    OUT local_blks_read int8,
    OUT local_blks_written int8,
    OUT temp_blks_read int8,
    OUT temp_blks_written int8,
    OUT last_startup_cost float8,
    OUT last_total_cost float8
)
RETURNS SETOF record
AS '$libdir/pg_stat_plans'
LANGUAGE C COST 1000;

CREATE FUNCTION pg_stat_plans_explain(planid oid,
							userid oid default NULL,
							dbid oid default NULL,
							encodingid oid default NULL)
RETURNS TEXT
AS '$libdir/pg_stat_plans'
LANGUAGE C;

CREATE FUNCTION pg_stat_plans_pprint(sqltext text)
RETURNS TEXT
AS '$libdir/pg_stat_plans'
STRICT LANGUAGE C;

-- Register a view on the function for ease of use.
CREATE VIEW pg_stat_plans AS
  SELECT * FROM pg_stat_plans();

CREATE VIEW pg_stat_plans_queries AS
  SELECT
	userid,
	dbid,
	-- XXX: The order of array_agg output is undefined. However, in practice it
	-- is safe to assume that the order will be consistent across array_agg calls
	-- in this query, so that plan_ids will correspond to calls_per_plan.
	array_agg(planid) AS plan_ids,
	array_agg(calls) AS calls_per_plan,
	array_agg(total_time / calls) AS avg_time_per_plan,
	normalize_query(query) AS normalized_query,
	sum(calls) AS calls,
	sum(total_time) AS total_time,
	variance(total_time / calls) AS time_variance,
	stddev_samp(total_time/ calls) AS time_stddev,
	sum(rows) AS rows,
	sum(shared_blks_hit) AS shared_blks_hit,
	sum(shared_blks_read) AS shared_blks_read,
	sum(shared_blks_written) AS shared_blks_written,
	sum(local_blks_hit) AS local_blks_hit,
	sum(local_blks_read) AS local_blks_read,
	sum(local_blks_written) AS local_blks_written,
	sum(temp_blks_read) AS temp_blks_read,
	sum(temp_blks_written) AS temp_blks_written
  FROM pg_stat_plans()
	GROUP BY
	1, 2, 6;

GRANT SELECT ON pg_stat_plans TO PUBLIC;
GRANT SELECT ON pg_stat_plans_queries TO PUBLIC;

-- Don't want these to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_plans_reset() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_stat_plans_pprint(text) FROM PUBLIC;
