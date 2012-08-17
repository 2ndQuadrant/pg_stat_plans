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
