-- -----------------------------------------------------------------------------
-- samples.sql
--
--    Sample queries that show idiomatic use of pg_find_plans infrastructure.
--
--	Copyright (c) 2013, 2ndQuadrant Ltd.
--
--
-- -----------------------------------------------------------------------------

-- These queries assume that materialize_plans() is called frequently, so that
-- materialized JSON explain text is available for all pg_stat_plans entries (or
-- at least the large majority). See pg_find_plan's README.rst file for more
-- information on how to ensure that this happens.
--
-- Show executions cost for plans that feature a sequential scan against the
-- table "orders". Note that regclass is used as the type of an argument to
-- various functions, to sanitise input (i.e. if your database doesn't have an
-- orders table, this query will error):
select
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
and
	contains_node(json_plan, 'Seq Scan', 'orders');

-- Show deletes against the 'orders' table, too.
select
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
and
	contains_modifytable(json_plan, 'Delete', 'orders');

-- Show execution costs for plans that feature a sequential scan against
-- whatever the 5 largest tables in the database happen to be:
select
	-- About how many blocks are affected by each call? (rank + value):
	rank() over (order by shared_blks_hit + shared_blks_read desc) as total_blocks_affected_rank,
	shared_blks_hit + shared_blks_read as total_blocks_affected,
	coalesce(nullif(shared_blks_hit + shared_blks_read, 0), 0) / calls as blocks_affected_per_call,
	rank() over (order by shared_blks_hit) as shared_blks_hit_rank,
	rank() over (order by shared_blks_read) as shared_blks_read_rank,
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
and
	contains_nodes(json_plan, 'Seq Scan',
	(select array_agg(r) from
      (select nspname || '.' || relname r from pg_class c
	   left join pg_namespace n on n.oid = c.relnamespace
	   order by pg_relation_size(c.oid) desc limit 5) s
	)
				)
order by
	-- total number of blocks affected (not per call)
	1 asc;

-- Show execution costs for the 5 plans that have the largest number of joins
-- (Note: Postgres uses pair-wise joins, so if there are n tables to join
-- together there will be n-1 joins):
select
	-- Could limit this to inner joins by changing second arg to join_count():
	join_count(json_plan),
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
order by
	1 desc nulls last limit 5;

-- Show execution costs for plans with a number of joins that exceeds
-- join_collapse_limit:
select
	join_count(json_plan),
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
and
	join_count(json_plan) >
	(select setting::int from pg_settings where name = 'join_collapse_limit');

-- Show stored plans whose query text, when previously explained, did not
-- produce the same plan as that of the entry itself. This may be due to their
-- query having crossed a crossover point:
select
	query,
	error_seen,
	calls
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	sp.json_plan is null
or
	-- Hopefully this shows errors, but not old ones.
	(sp.error_seen is not null and
		(p.last_startup_cost != sp.last_startup_cost or
			p.last_total_cost != sp.last_total_cost));

-- Show entries within pg_stat_plans that do not yet have an entry within the
-- stored_plans table (i.e. there is no plan text stored, nor is there any error
-- or record of having attempted processing), typically because
-- materialize_plans() has not been run since their introduction into the
-- pg_stat_plans shared hash table.
select
	query,
	calls
from
	pg_stat_plans p
left join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	sp.error_seen is null and sp.json_plan is null;

-- Find aggregates of the "Hashed" strategy (i.e. HashAggregates). Should
-- contain Seq Scan against the orders table too.
select
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
and
	contains_aggregate(json_plan, 'Hashed')
and
	contains_node(json_plan, 'Seq Scan', 'orders');

-- Find plan costs for plans that involve the table 'orders' in any way
select
	p.*
from
	pg_stat_plans p
	join
	stored_plans sp on (p.userid=sp.userid and p.dbid=sp.dbid and p.planid=sp.planid)
where
	from_our_database
and
	contains_node(json_plan, relation := 'orders');
