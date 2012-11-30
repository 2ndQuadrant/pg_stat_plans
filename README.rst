==================================================================================
pg_stat_plans: pg_stat_statements variant that differentiates between query plans.
==================================================================================

Version: 1.0 beta 2

Author: Peter Geoghegan

        peter@2ndquadrant.com

Based on an idea by Peter Geoghegan and Simon Riggs.

Introduction
============

pg_stat_plans is a variant of the standard Postgres contrib module
pg_stat_statements. It differentiates between and assigns execution costs to
plans, rather than queries. This makes it particularly suitable for monitoring
query planner regressions. However, it is also suitable as a general-purpose
tool for understanding execution costs at both the plan and query granularity.

The tool is a derivative of the pg_stat_statements module that is distributed
with Postgres 9.2. An explicit goal of pg_stat_plans is to support Postgres 9.0
and 9.1 - Postgres 9.2's pg_stat_statements module does not support earlier
versions of the server, and contrib modules are generally only intended to be
used with the same version of the server that they're distributed with.

pg_stat_plans is also intended to support advanced use-cases, including the
aggregation of statistics by third-party tools. This is why the planid value is
exposed for each entry (notably, the queryid value in pg_stat_statements is
*not* exposed). It is possible that a future version of pg_stat_plans will have
explicit support for querying entries based on criteria like if a certain index
was used, and the support for machine-readable explain formats (e.g.  JSON,
YAML) anticipates this.

Supported PostgreSQL versions
=============================

pg_stat_plans strives to support as many community-supported major versions of
Postgres as possible. Currently, the following versions of PostgreSQL are
supported:

9.0 (see notes on search_path), 9.1, 9.2

9.3-devel is unsupported, but we strive to build cleanly against it in
anticipation of supporting it as soon as that's useful.

Installation
============

The module can be built using the standard PGXS infrastructure. For this to
work, you will need to have the ``pg_config`` program available in your $PATH. When
using the PGDG Redhat RPMs, you will need to install a separate package to have
this available.

Using PGXS
----------

If you are using a packaged PostgreSQL build and have ``pg_config`` available
(and in your OS user's $PATH), the procedure is as follows::

  tar xvzf pg_stat_plans-1.0.tar.gz
  ...
  cd pg_stat_plans-1.0
  make
  make install

See below for building notes specific to Redhat Linux variants.

Note that just because ``pg_config`` is located in one user's $PATH does not
necessarily make it so for the root user. A workaround is described below,
at the end of the Redhat notes.

The pg_stat_plans module must be created in PostgreSQL. See "setting up
PostgreSQL", below.

Notes on RedHat Linux, Fedora, and CentOS Builds
------------------------------------------------

The offical PGDG RPM PostgreSQL packages put ``pg_config`` into the
``postgresql-devel`` package, not the main server package. If you have a RPM
install of PostgreSQL 9.1, the PostgreSQL binary directory will not be in your
PATH by default either. Individual utilities are made available via the
``alternatives`` mechanism, but not all commands will be available that way.

When building pg_stat_plans against a RPM packaged build, you may discover that
some development packages are needed as well. The following build errors can
occur::

  /usr/bin/ld: cannot find -lxslt
  /usr/bin/ld: cannot find -lpam

Install the following packages to correct those::

  yum install libxslt-devel
  yum install pam-devel

When building pg_stat_plans using a regular user account, installing into the
system directories using sudo, ``pg_config`` won't be in root's path.

The following invocation of ``make`` works around this issue::

  sudo PATH="/usr/pgsql-9.1/bin:$PATH" make install

Notes on Debian and Ubuntu Builds
---------------------------------

The Makefile also provides a target for building Debian packages. The target has
a dependency on ``debhelper`` and ``postgresql-server-dev-all``, and the
PostgreSQL source package itself (e.g. ``postgresql-server-dev-9.2``).

The packages can be created and installed as follows::

  sudo aptitude install debhelper postgresql-server-dev-all
  make deb
  sudo dpkg -i ../postgresql-9.2-pgstatplans_*.deb

Setting up PostgreSQL
---------------------

The module requires additional shared memory amounting to about
pg_stat_plans.max * plans_query_size bytes. Note that this memory is
consumed whenever the module is loaded, even if pg_stat_plans.track is set
to none.

It is necessary to change a setting in postgresql.conf. The module must be loaded
by adding pg_stat_plans to shared_preload_libraries in postgresql.conf, because
it requires additional shared memory. This means that a server restart is needed
to add or remove the module. Typical usage might be::

  # postgresql.conf
  shared_preload_libraries = 'pg_stat_plans'
  # Optionally:
  pg_stat_plans.max = 10000
  pg_stat_plans.track = all

Note that if necessary, pg_stat_plans can co-exist with pg_stat_statements.
However, the redundant fingerprinting of queries may impose an unreasonable
overhead.

pg_stat_plans objects must be installed in every database that they are
required. It uses the PostgreSQL extension mechanism where available. To install
on PostgreSQL versions 9.1+, execute the following SQL command::

  mydb=# CREATE EXTENSION pg_stat_plans;

Earlier releases (that lack the extension mechanism - in practice, this is
limited to version 9.0) must create the extension by executing the SQL script
directly::

  psql mydb -f pg_stat_plans.sql

Usage
=====

pg_stat_plans, once installed, creates the following objects (plus a few others
that are not intended to be used by the user directly).

For security reasons, non-superusers are not allowed to see the text of queries
executed by other users. They can see the statistics, however, if the view has
been installed in their database.

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
| query               | text             | Text of the first statement (up to plans_query_size bytes)          |
+---------------------+------------------+---------------------------------------------------------------------+
| had_our_search_path | boolean          | Indicates if query strings execution's search_path matches current  |
+---------------------+------------------+---------------------------------------------------------------------+
| from_our_database   | boolean          | Indicates if the entry originated from the current database         |
+---------------------+------------------+---------------------------------------------------------------------+
| query_valid         | boolean          | Indicates if query column text now produces same plan               |
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
| last_startup_cost   | double precision | Last plan start-up cost observed for entry                          |
+---------------------+------------------+---------------------------------------------------------------------+
| last_total_cost     | double precision | Last plan total cost observed for entry                             |
+---------------------+------------------+---------------------------------------------------------------------+

The columns (userid, dbid, planid) serve as a unique identifier for each
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

 pg_stat_plans_reset()

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

Arguments to the pg_stat_plans_explain function should correspond to the set of
values that together uniquely identify some entry currently within the
pg_stat_plans view. It is possible to omit all but the planid argument - the
default argument of NULL for userid, dbid and encodingid is interpreted by
pg_stat_plans as the current value in each case, whatever that may be (that is,
the current connection's user and database identifiers, and the backend
encoding).

Usage example::

  postgres=# select pg_stat_plans_explain(planid, userid, dbid),
      planid, last_startup_cost, last_total_cost from pg_stat_plans
      where from_our_database and planid = 2721250187;
  -[ RECORD 1 ]---------+--------------------------------------------------
  pg_stat_plans_explain | Result  (cost=0.00..0.01 rows=1 width=0)
  planid                | 2721250187
  last_startup_cost     | 0
  last_total_cost       | 0.01

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
simple shift in the planner's preferences; in general a shift back remains quite
possible.

The first time that a query is invalidated, a WARNING message is raised. It may
be possible to observe the point at which the planner begins to prefer an
alternative plan (the "crossover point") by referring to the
``last_startup_cost`` and/or ``last_total_cost`` columns for each entry (among a
set of entries related to the same query). Note, however, that this information
should be interpreted carefully. It should be considered, for example, that it
is quite possible for the planner to conclude that a certain plan is optimal,
when that plan can be shown to actually be quite sub-optimal, due to the
planner's choices being predicated on outdated statistics (to determine if this
is happening, a manual ``EXPLAIN ANALYZE`` - which shows estimated and *actual*
costs - is often very helpful). When those statistics are subsequently updated
(perhaps by running ``ANALYZE`` manually), the planner may indicate that the
new, superior plan actually has a higher estimated cost than the old, inferior
plan.

Note that there are numerous caveats related to this function. They are noted
separately below, under "Limitations".

pg_stat_plans_queries view
--------------------------

A variant of the regular pg_stat_plans view that summarises the statistics at
the query granularity. Regular expression query text normalization, with all of
the attendant limitations is used.

Most columns are essentially equivalent to and directly derived from a
pg_stat_plans column, and as such are not described separately. Some of the
views' columns, whose broad purpose is to facilitate finding outlier plans, are
described below:

+---------------------+-----------+---------------------------------------------------------------+
| Name                | Type      | Description                                                   |
+=====================+===========+===============================================================+
| plan_ids            | oid[]     | planids for all plans of the statement                        |
+---------------------+-----------+---------------------------------------------------------------+
| calls_per_plan      | integer[] | Corresponding calls for each plan                             |
+---------------------+-----------+---------------------------------------------------------------+
| avg_time_per_plan   | integer[] | Corresponding average time (in milliseconds) for each plan    |
+---------------------+-----------+---------------------------------------------------------------+
| normalized_query    | text      | Query text, normalized with simple regular expression method  |
+---------------------+-----------+---------------------------------------------------------------+
| time_variance       | double    | Variance in average execution times for each plan             |
+---------------------+-----------+---------------------------------------------------------------+
| time_stddev         | double    | Stddev of average execution times for each plan               |
+---------------------+-----------+---------------------------------------------------------------+

Note that because ``pg_stat_plans_queries`` is defined in terms of
pg_stat_plans, it is possible for one plan to be evicted from the module's
shared hash table, while another plan associated with the same query remains,
giving a set of execution costs for the query that are not really representative
of actual costs since the query was first instrumented.

Configuration Parameters
========================

pg_stat_plans adds the following configuration parameters:

``pg_stat_plans.max (integer)``
-------------------------------
pg_stat_plans.max is the maximum number of plans tracked by the module (i.e.,
the maximum number of rows in the pg_stat_plans view). If more distinct plans
than that are observed, information about the least-executed statements is
discarded. The default value is 1000. This parameter can only be set at server
start.

``pg_stat_plans.track (enum)``
------------------------------
pg_stat_plans.track controls which statements' plans are counted by the module.
Specify top to track top-level statements (those issued directly by clients),
all to also track nested statements (such as statements invoked within
functions), or none to disable plan statistics collection. The default
value is top. Only superusers can change this setting.

``pg_stat_plans.save (boolean)``
--------------------------------
pg_stat_plans.save specifies whether to save plan statistics across server
shutdowns. If it is off then statistics are not saved at shutdown nor reloaded
at server start. The default value is on. This parameter can only be set in the
postgresql.conf file or on the server command line.

``pg_stat_plans.planid_notice (boolean)``
-----------------------------------------
Raise notice of a plan's id after its execution. Useful for verifying explain
output on an ad-hoc basis. The default is off. The setting can be changed by
users dynamically.

``pg_stat_plans.explain_format (enum)``
-----------------------------------
pg_stat_plans.explain_format selects the EXPLAIN output format to be used (i.e
the format that will be returned by ``pg_stat_plans_explain()``). The allowed
values are text, xml, json, and yaml. The default value is text. The setting can
be changed by users dynamically.

``pg_stat_plans.verbose (boolean)``
-----------------------------------
pg_stat_plans.verbose specifies if explain output should be verbose (that is,
equivalent to specifying VERBOSE with SQL EXPLAIN). The default value is off.
The setting can be changed by users dynamically.

``pg_stat_plans.plans_query_size (integer)``
----------------------------------
Controls the length in bytes of the stored SQL query string. Because truncating
the stored strings prevents subsequently explaining the entry, it may be
necessary to increase this value. The default value is 2048. This parameter can
only be set at server start.

Limitations
===========

Plan fingerprinting
-------------------

pg_stat_plans works by hashing query plans. While that makes it more useful than
Postgres 9.2's pg_stat_statements in some respects (it is possible to directly
monitor planner regressions), most of the limitations of the tool are a natural
consequence of this fact.

For example, the following two queries are considered equivalent by the module::

  select upper(lower('text'));
  select upper(upper('text'));

This is because the underlying ``pg_proc`` accessible functions are actually
executed in preprocess_expression during planning, not execution proper. By the
time the executor hook of pg_stat_plans sees the Node, it appears to be a simple
Const node, and it is impossible to work backwards to the original
representation.

However, the module can differentiate between these queries just fine::

  select upper(lower(firstname)) from customers;
  select upper(upper(firstname)) from customers;

The fact that this sort of thing can occur has the potential to be very
confusing for some edge cases. Consider this example::

  set pg_stat_plans.track = 'all';

  ...

  create or replace function bar(f integer) returns integer as
  $$
      DECLARE
          ret integer;
      BEGIN
          select case f when 0 then 0 else bar(f -1) end into ret;
          RETURN ret;
      END;
  $$ language plpgsql;

  ...

  select bar(5);

The way that the execution costs involved here actually get broke out is
version-dependent (though on any version, pg_stat_plans still attributes costs
to the actual plans executed). Postgres 9.2+ added this feature::

  Allow the planner to generate custom plans for specific parameter values even
  when using prepared statements.

For this reason, the recursive query happens to have the same finished plan as
the top-level direct call to the function (even though it would have a distinct
query fingerprint, if pg_stat_statements was consulted). At the same time, the
terminating execution (again, because of the custom plan feature; pl/pgsql uses
prepared statements under-the-hood) has a *different* plan to every other plan
(different to both all other executions of that same prepared query, as well as
the top-level call "select case f when 0 then 0 else bar(f -1) end").

The final result is a top-level call and all-but-one recursive calls bunched
together into a single entry, while the terminating call is in another entry.
This *looks* like the top-level query is broken out from the recursive queries
(and that the entry just has the wrong query text - both entries have "select
case f when 0 then 0 else bar(f -1) end"), but in actuality everything has the
right query text. The plan with a single call just isn't the plan it appears to
be at first.

On 9.1, however, the behaviour of pg_stat_plans here happens to be more
intuitive. That is, as would be the case with 9.2's pg_stat_statements, the
top-level query forms one entry, and all recursive queries another, since the
recursive queries always use the same generic plan on that version.

Explaining stored query text
----------------------------

The module will not explain stored query text that has been truncated. For that
reason, the size of stored query text is set separately from the server-wide
``track_activity_query_size`` setting. It may be necessary to set
``pg_stat_plans.plans_query_size`` to a value greater than the default of 2048.

pg_stat_plans EXPLAINs plans using a standard interface with the stored query
text. Since there is no way to explain the stored query text of a query prepared
using ``PQPrepare()``, there is no reasonable way to handle that case, and it is
not supported. If the query string had PARAM placeholder tokens replaced with
actual textual constants, this would still not result in an equivalent query
plan, at least as far as our fingerprinting is concerned. This isn't a serious
limitation, since presumably those that are particularly concerned about planner
regressions don't use prepared statements. Note that pg_stat_plans will assign
execution costs to these prepared statement plans just as readily as any other
type of plan.

The query text may not adequately represent the originating query for each plan.
In particular, inconsistently setting the ``search_path`` setting may allow what
appears to be the same query to be misidentified as another query referring to
what are technical other relations. This isn't at all unreasonable, since
"schema naivety" is encouraged in application code. For that reason, a
fingerprint of the search_path setting is stored with each pg_stat_plans entry.

The module will produce an error in the event of trying to call
pg_stat_plans_explain function (which rather straightforwardly explains the
stored query text of the originating query's execution) with a different
``search_path`` setting to that used for the original execution, if and only if
the plan fingerprinting shows an inconsistency (if the ``search_path`` setting
matched, the inconsistency would only result in a warning, as it would be
assumed that the query proper remained the same). The ``had_our_search_path``
column of the pg_stat_plans view indicates if this will happen for the entry
should the function be called. Note, however, that due to a technical
limitation, support for this is not available for PostgreSQL 9.0, and on that
version the ``had_our_search_path`` column will always be NULL.

Utility statements
------------------
pg_stat_plans does not retain pg_stat_statements ability to separately track
utility statements. One reason for this is that it would create a tension with
how and where we count some other types of execution costs (some utility
statements have plans associated with them, which are separately executed).

Possibility of hash collisions, stability of planids
----------------------------------------------------

pg_stat_plans inherits some limitations from pg_stat_statements. In some cases,
plans that have significantly different query texts might get merged into a
single pg_stat_plans entry. Normally this will happen only because plans are
substantively equivalent, but there is a small chance of hash collisions causing
unrelated plans to be merged into one entry (that is, for their ``planid`` value
to match despite the differences). However, this cannot happen with plans that
belong to different users or databases.

pg_stat_plans fingerprints plans in a way that is sensitive to implementation
details like machine endian-ness, as well as the values of internal object
identifiers. For that reason, it should not be assumed that planids can be used
to identify plans across servers participating in *logical* replication of the
same database, or that planids will be consistent across a dump and reload
cycle, or Postgres versions. However, planids will be consistent when using
physical replication (that is, streaming replication) or physical backups.

It is a goal of pg_stat_plans to facilitate the aggregation of statistics by
third-party tools based on using planids as persistent identifiers. For that
reason, but also because an internal "version-bump" that invalidates all
existing entries is best avoided, the author will strive to keep the
fingerprinting logic that produces planids stable across releases. However, it
is *not* guaranteed that planids will be consistent across versions of
pg_stat_plans, mostly because it is conceivable that the internal representation
of plans will be altered in a point-release of Postgres.
