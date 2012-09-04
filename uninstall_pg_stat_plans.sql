-- This must be ordered to respect object dependencies.
DROP VIEW pg_stat_plans;
DROP VIEW pg_stat_plans_queries;

DROP FUNCTION normalize_query(IN TEXT, OUT TEXT);
DROP FUNCTION pg_stat_plans_reset();
DROP FUNCTION pg_stat_plans(
    OUT oid,
    OUT oid,
    OUT oid,
    OUT text,
    OUT boolean,
    OUT boolean,
    OUT int8,
    OUT float8,
    OUT int8,
    OUT int8,
    OUT int8,
    OUT int8,
    OUT int8,
    OUT int8,
    OUT int8,
    OUT int8,
    OUT int8
);
DROP FUNCTION pg_stat_plans_explain(oid, oid, oid, oid);
DROP FUNCTION pg_stat_plans_pprint(sqltext text);
