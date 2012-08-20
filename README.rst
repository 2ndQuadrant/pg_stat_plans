==================================================================================
pg_stat_plans: pg_stat_statements variant that differentiates between query plans.
==================================================================================

Version: 0.1

Author: Peter Geoghegan

        peter@2ndquadrant.com

Introduction
============

pg_stat_plans is a variant of the standard Postgres contrib module,
pg_stat_statements. It differentiates plans, rather than queries.

The tool is a derivative of the pg_stat_statements available from PostgreSQL
9.2. However, an explicit goal of the tool is to work with PostgreSQL 9.1.

Installation
============
The module can be installed using the standard PGXS infrastructure.

Limitations
===========

pg_stat_plans works by hashing query plans. While that makes it more useful than
Postgres 9.2's pg_stat_statements in some respects (it is possible to directly
monitor planner regressions), most of the obvious limitations of the tool are a
natural consequence of this fact.

For example, the following two queries are considered equivalent by the module:

select upper(lower("text"));

select upper(upper("text"));

This is because the underlying pg_proc accessible functions are actually
executed in preprocess_expression during planning, not execution proper. By the
time the executor hook of pg_stat_plans sees the Node, it appears to be a simple
Const node, and it is impossible to work backwards.

However, the module can differentiate between these queries just fine:

select upper(lower(firstname)) from customers;

select upper(upper(firstname)) from customers;
