/* pg_stat_plans/pg_stat_plans--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_plans" to load this file. \quit

-- Register functions.
CREATE FUNCTION pg_stat_plans_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_stat_plans(
    OUT userid oid,
    OUT dbid oid,
    OUT planid oid,
    OUT query text,
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
    OUT temp_blks_written int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Register a view on the function for ease of use.
CREATE VIEW pg_stat_plans AS
  SELECT * FROM pg_stat_plans();

GRANT SELECT ON pg_stat_plans TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_plans_reset() FROM PUBLIC;
