-- -----------------------------------------------------------------------------
-- pg_stat_plans.sql
--
--    Install pg_stat_plans extension module.
--
--	Copyright (c) 2012, 2ndQuadrant Ltd.
--
--
-- -----------------------------------------------------------------------------

-- Originally from http://blog.ioguix.net
create or replace function normalize_query(in text, out text) as
$fun$
  select
	regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(

    $1,

    -- Replace all whitespace with a single space
    $$\s+$$,						' ',			'g'		),

    -- Remove string content
    $$\\'$$,						'',				'g'		),
    $$'[^']*'$$,					$$?$$,			'g'		),
    $$''('')+$$,					$$?$$,			'g'		),

    -- Remove null parameters
    E'=\\s*NULL',					'= ?',			'gi'	),

    -- Remove numbers
    '([^a-z_$-])-?([0-9]+)',		E'\\1'||'?',	'g'		),

    -- Remove hexadecimal numbers
    '([^a-z_$-])0x[0-9a-f]{1,10}',	E'\\1'||'?',	'g'		),

    -- Remove in() values, as best we can
    E'in\\s*\\([''0x,\\s]*\\)',		'in (...)',		'g'		);
$fun$
strict immutable language sql;

-- Register functions.
create or replace function pg_stat_plans_reset()
returns void
as '$libdir/pg_stat_plans'
language c;

create or replace function pg_stat_plans(
    out planid oid,
    out userid oid,
    out dbid oid,
    out query text,
    out had_our_search_path boolean,
    out from_our_database boolean,
    out query_explainable boolean,
    out calls int8,
    out total_time float8,
    out rows int8,
    out shared_blks_hit int8,
    out shared_blks_read int8,
    out shared_blks_written int8,
    out local_blks_hit int8,
    out local_blks_read int8,
    out local_blks_written int8,
    out temp_blks_read int8,
    out temp_blks_written int8,
    out blk_read_time float8,
    out blk_write_time float8,
    out last_startup_cost float8,
    out last_total_cost float8
)
returns setof record
as '$libdir/pg_stat_plans'
language c cost 1000;

create or replace function pg_stat_plans_explain(planid oid,
							userid oid default null,
							dbid oid default null,
							encodingid oid default null)
returns text
as '$libdir/pg_stat_plans'
language c;

create or replace function pg_stat_plans_pprint(sqltext text)
returns text
as '$libdir/pg_stat_plans'
strict language c;

-- Register a view on the function for ease of use.
create view pg_stat_plans as
  select * from pg_stat_plans();

create view pg_stat_plans_queries as
  select
	array_agg(planid order by planid) AS planids,
	userid,
	dbid,
	array_agg(calls order by planid) AS calls_per_plan,
	array_agg(total_time / calls order by planid) AS avg_time_per_plan,
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
	sum(temp_blks_written) AS temp_blks_written,
	sum(blk_read_time) AS blk_read_time,
	sum(blk_write_time) AS blk_write_time
  from pg_stat_plans()
	group by
	2, 3, 6;

grant select on pg_stat_plans to public;
grant select on pg_stat_plans_queries to public;

-- Don't want these to be available to non-superusers.
revoke all on function pg_stat_plans_reset() from public;
revoke all on function pg_stat_plans_pprint(text) from public;
