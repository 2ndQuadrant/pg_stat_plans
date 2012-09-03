==================================================================================
pg_stat_plans: pg_stat_statements variant that differentiates between query plans.
==================================================================================

Version: 0.1

Author: Peter Geoghegan

        peter@2ndquadrant.com

Introduction
============

pg_stat_plans is a variant of the standard Postgres contrib module,
pg_stat_statements. It differentiates between and assigns execution costs to
plans, rather than queries. This makes it particularly suitable for monitoring
query planner regressions. However, it is also suitable as a general-purpose
tool for understanding execution costs at both the plan and query granularity.

The tool is a derivative of the pg_stat_statements module that is distributed
with Postgres 9.2. An explicit goal of pg_stat_plans is to support Postgres 9.0
and 9.1 - Postgres 9.2's pg_stat_statements module does not support earlier
versions of the server, and contrib modules are generally only intended to be
used with the same version of the server that they're distributed with.

Supported PostgreSQL versions
=============================

pg_stat_plans strives to support as many community-supported major versions of
Postgres as possible. Currently, the following versions of PostgreSQL are
supported:

9.2
9.1

Earlier versions may be supported if there is sufficient demand.

Installation
============

There are two basic approaches to building pg_stat_plans from source.

The module can be built using the standard PGXS infrastructure. For this to
work, you will need to have the ``pg_config`` program available in your $PATH. When
using the PGDG Redhat RPMs, you will need to install a separate package to have
this available.

You may also wish to build pg_stat_plans using a full PostgreSQL source code
tree. This is generally only desirable if you're installing PostgreSQL from
source.

Using PGXS
----------

If you are using a packaged PostgreSQL build and have ``pg_config`` available
(and in your OS user's $PATH), the procedure is as follows::

  tar xvzf pg_stat_plans-1.0.tar.gz
  cd pg_stat_plans
  make USE_PGXS=1
  make USE_PGXS=1 install

This is the preferred method of building pg_stat_plans for production use.

See below for building notes specific to Redhat Linux variants.

Note that just because ``pg_config`` is located in one user's $PATH does not
necessarily make it so for the root user. A workaround is described below,
at the end of the Redhat notes.

Using a PostgreSQL source code tree
-----------------------------------

With this method, the pg_stat_plans code is copied into the PostgreSQL tree.

The resulting subdirectory should be named ``contrib/pg_stat_plans``, without any
version number::

  cp pg_stat_plans-1.0.tar.gz ${postgresql_sources}/contrib
  cd ${postgresql_sources}/contrib
  tar xzvf pg_stat_plans-1.0.tar.gz
  mv pg_stat_plans-1.0 pg_stat_plans
  cd pg_stat_plans
  make
  make install

Notes on RedHat Linux, Fedora, and CentOS Builds
------------------------------------------------

The offical PGDG RPM PostgreSQL packages put ``pg_config`` into the
``postgresql-devel`` package, not the main server package. If you have a RPM
install of PostgreSQL 9.1, the PostgreSQL binary directory will not be in your
PATH by default either. Individual utilities are made available via the
``alternatives`` mechanism, but not all commands will be available that way.

When building repmgr against a RPM packaged build, you may discover that some
development packages are needed as well. The following build errors can occur::

  /usr/bin/ld: cannot find -lxslt
  /usr/bin/ld: cannot find -lpam

Install the following packages to correct those::

  yum install libxslt-devel
  yum install pam-devel

When building pg_stat_plans using a regular user account, installing into the
system directories using sudo, ``pg_config`` won't be in root's path.

The following invocation of ``make`` works around this issue::

  sudo PATH="/usr/pgsql-9.1/bin:$PATH" make USE_PGXS=1 install

Usage
=====

pg_stat_plans, once installed, creates the following objects (plus a few others
that are not intended to be used by the user directly):

pg_stat_plans view
------------------

Summarises execution costs of each plan executed. Each entry represents a
discrete plan. Each distinct query may have multiple entries (one for each
plan executed).

+---------------------+------------------+---------------------------------------------------------------------+
| Name                | Type             | Description                                                         |
+=====================+==================+=====================================================================+
| userid              | oid              | OID of user who executed the plan                                   |
+---------------------+------------------+---------------------------------------------------------------------+
| dbid                | oid              | OID of database in which the plan was executed                      |
+---------------------+------------------+---------------------------------------------------------------------+
| planid              | oid              | OID of the plan                                                     |
+---------------------+------------------+---------------------------------------------------------------------+
| query               | text             | Text of the first statement (up to track_activity_query_size bytes) |
+---------------------+------------------+---------------------------------------------------------------------+
| query_valid         | boolean          | indicates if query column text now produces same plan               |
+---------------------+------------------+---------------------------------------------------------------------+
| calls               | bigint           | Number of times executed                                            |
+---------------------+------------------+---------------------------------------------------------------------+
| total_time          | double precision | Total time spent in execution, in milliseconds                      |
+---------------------+------------------+---------------------------------------------------------------------+
| rows                | bigint           | Total number of rows retrieved or affected by the plan              |
+---------------------+------------------+---------------------------------------------------------------------+
| shared_blks_hit     | bigint           | Total number of shared blocks hits by the plan                      |
+---------------------+------------------+---------------------------------------------------------------------+
| shared_blks_read    | bigint           | Total number of shared blocks reads by the plan                     |
+---------------------+------------------+---------------------------------------------------------------------+
| shared_blks_written | bigint           | Total number of shared blocks writes by the plan                    |
+---------------------+------------------+---------------------------------------------------------------------+
| local_blks_hit      | bigint           | Total number of local blocks hits by the plan                       |
+---------------------+------------------+---------------------------------------------------------------------+
| local_blks_read     | bigint           | Total number of local blocks reads by the plan                      |
+---------------------+------------------+---------------------------------------------------------------------+
| local_blks_written  | bigint           | Total number of local blocks writes by the plan                     |
+---------------------+------------------+---------------------------------------------------------------------+
| temp_blks_read      | bigint           | Total number of temp blocks reads by the plan                       |
+---------------------+------------------+---------------------------------------------------------------------+
| temp_blks_written   | bigint           | Total number of temp blocks writes by the plan                      |
+---------------------+------------------+---------------------------------------------------------------------+

The columns (userid, dbid, planid) serve as a unique indentifier for each
entry in the view (assuming consistent use of a single encoding). planid is a
value derived from hashing the query tree just prior to execution.

query_valid is false if and only if an execution of the pg_stat_plans_explain
function previously found that explaining the original query text did not
produce the expected query plan for the entry. During the next execution of
the plan (at some indefinite point in the future), the query column's contents
will be replaced by new query text, and will be re-validated.

pg_stat_plans_reset function
----------------------------

Can be called by superusers to reset the contents of the pg_stat_plans view
(and, by extension, all others views based on it)::

 pg_stat_plans_reset() -- no arguments should be given

pg_stat_plans_explain function
------------------------------
The function displays text output of explaining the query with the constants
that appeared in the original execution of the plan::

 pg_stat_plans_explain(planid oid NOT NULL, userid oid default NULL, dbid oid
            default NULL, encodingid oid default NULL) returns TEXT

Note that all arguments other than ``planid`` have a default argument of NULL.
In this context, NULL is interpreted as the current (userid|dbid|encodingid).

Much of the time, the query plan generated will be the same as the plan
originally executed when the entry was created. This is certainly not guaranteed
though.  Even though the constants and query itself are the same, the
selectivity of those constants may have changed, we may now have superior (or
even inferior) statistics, and the planner may have access to indexes that were
not previously available. In short, the plan may have changed for a great number
of reasons, and that should be highlighted. This is intended to be a practical
alternative to actually storing all plans executed against the database.

This function can be used to monitor planner regressions.

Arguments should correspond to the respective values of some entry within the
pg_stat_plans view. It is possible to omit all but the planid argument - the
default argument of 0 for all other values is interpret by pg_stat_plans as the
current value.

Usage example::

 postgres=# select pg_stat_plans_explain(planid, userid, dbid), planid from
  pg_stat_plans;
 -[ RECORD 1 ]---------+--------------------------------------------------
 pg_stat_plans_explain | Query Text: EXPLAIN select pg_stat_plans_reset();
                       | Result  (cost=0.00..0.01 rows=1 width=0)
 planid                | 2721250187

Internally, the function simply executes an ``EXPLAIN`` (*not* an ``EXPLAIN
ANALYZE``) based on the known query text.

If the known query text now produces a plan that is not the same as the entry's
actual plan, the query text is automatically *invalidated*. Its ``query_valid``
column within pg_stat_plans will subsequently have a value of ``false``.

The invalid query string is automatically replaced by a now-valid string for the
plan at the next opportunity (i.e. if and when the original plan is once again
executed). When this occurs, the entry is revalidated.

Consider the following scenario:

A query is executed. The selectivity estimate of the constants seen in this
original execution of the query/plan result in a pg_stat_plans entry.
Subsequently, though that plan may continue to be used for certain other
constant values, a shift in statistical distribution happened to result in it
not being used for the originally seen constant value(s). This is why we
optimistically allow for the plan's revalidation. It would be unhelpful to
discard statistics for plans that we may not see again, if this is due to a
simple shift in the planner's preferences.

The first time that a query is invalidated, a WARNING message is raised.

Note that there are numerous caveats related to this function. They are noted
separately below, under "Limitations".

pg_stat_plans_queries view
--------------------------

A variant of the regular pg_stat_plans view that summarises the statistics at
the query granularity. Regular expression query text normalisation, with all of
the attendant limitations is used.

Most columns are essentially equivalent to and directly derived from a
pg_stat_plans column, and as such are not described separately. Some of the
views' columns, whose broad purpose is to faciliate finding outlier plans, are
described below:

+---------------------+-----------+---------------------------------------------------------------+
| Name                | Type      | Description                                                   |
+=====================+===========+===============================================================+
| planids             | oid[]     | planids for all plans of the statement                        |
+---------------------+-----------+---------------------------------------------------------------+
| calls_histogram     | integer[] | Corresponding calls for each plan                             |
+---------------------+-----------+---------------------------------------------------------------+
| avg_time_historam   | integer[] | Corresponding average time (in milliseconds) for each plan    |
+---------------------+-----------+---------------------------------------------------------------+
| normalized_query    | text      | Query text, normalised with simple regular expression method. |
+---------------------+-----------+---------------------------------------------------------------+
| time_variance       | double    | Variance in average execution times for each plan.            |
+---------------------+-----------+---------------------------------------------------------------+
| time_stddev         | double    | Stddev of average execution times for each plan.              |
+---------------------+-----------+---------------------------------------------------------------+

Limitations
===========

Plan fingerprinting
~~~~~~~~~~~~~~~~~~~

pg_stat_plans works by hashing query plans. While that makes it more useful than
Postgres 9.2's pg_stat_statements in some respects (it is possible to directly
monitor planner regressions), most of the limitations of the tool are a natural
consequence of this fact.

For example, the following two queries are considered equivalent by the module::

  select upper(lower("text"));
  select upper(upper("text"));

This is because the underlying ``pg_proc`` accessible functions are actually
executed in preprocess_expression during planning, not execution proper. By the
time the executor hook of pg_stat_plans sees the Node, it appears to be a simple
Const node, and it is impossible to work backwards to the original
representation.

However, the module can differentiate between these queries just fine::

  select upper(lower(firstname)) from customers;
  select upper(upper(firstname)) from customers;

Explaining stored query text
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No particular effort is made by the module to ensure that it can explain a
truncated query text. If you run pg_stat_plans_explain on an entry whose query
text exceeds ``track_activity_query_size``, a syntax error may result. In fact,
it's possible (though quite unlikely) that there *will not* be a syntax error,
and an entirely distinct query will be explained, leading to a misrepresentation
of query execution costs.

Possibility of hash collisions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

pg_stat_plans inherits some limitations from pg_stat_statements. In some cases,
plans that have significantly different query texts might get merged into a
single pg_stat_plans entry. Normally this will happen only because plans are
substantively equivalent, but there is a small chance of hash collisions causing
unrelated plans to be merged into one entry (that is, for their ``planid`` value
to match despite the differences). However, this cannot happen with plans that
belong to different users or databases.
