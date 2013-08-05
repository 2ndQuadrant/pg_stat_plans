==================================================================================
pg_find_plans: Find plans in pg_stat_plans based on arbitrary criteria.
==================================================================================

Version: 1.0

Author: Peter Geoghegan

Introduction
============

pg_find_plans is an experimental submodule of pg_stat_plans. pg_stat_plans is a
framework for understanding plan execution costs, that by design exposes some
relatively low-level, extensible functionality, to be used as clients see fit -
plan costs are simply tracked in a view that the module makes available. Query
texts stored by the module can be explained on an ad-hoc basis (the module
manages some corner-cases with that), and machine-readable explain texts are
made available via a simple function-call API. For the most part, if users want
to ask complicated questions about plans, they have to figure out their own way
of doing so. Some users might be inclined to write queries like this, for
example::

  mydb=# select * from pg_stat_plans where query ilike '%select%mytable%';

This sort of approach is error-prone, rather limited in the types of questions
that can be asked, and generally unpolished. The pg_find_plans submodule exists
to provide users of pg_stat_plans with a higher-level, broadly useful set of
functionality for storing machine-readable explain plans, and subsequently
querying the plans in an *arbitrary* manner with confidence, to learn more about
plan execution costs.

pg_find_plans is written in PL/Python and PL/PgSQL. It is intended to provide
users with a better way to ask questions like "what are the execution costs of
all plans tracked since last statistics reset that involve a sequential scan
against ``mytable``, and have more than 2 joins?". That might be written as::

  mydb=# select
    join_count(json_plan),
    p.*
  from
    pg_stat_plans p
    join
    stored_plans sp on (p.planid=sp.planid and p.userid=sp.userid and p.dbid=sp.dbid)
  where
    from_our_database
  and
    join_count(json_plan) > 2
  and
    contains_node(json_plan, 'Seq Scan', 'mytable');
  order by
    1 desc nulls last;

Users should have a high degree of confidence that their queries on plan's
structure are free of detectable errors, and pg_find_plans ensures this by
carefully sanitizing user input. For example, if the node of interest was
specified as 'seq scan' above, the query would raise an error - to do any less
might result in a false sense of security about the actual costs of plans that
sequentially scan the table ``mytable``, since the implementation might then
naively ignore sequential scan nodes, as a case-sensitive comparison is used
internally. In general, making the interface hard to use incorrectly is even
more important than making it easy to use correctly.

Strictly speaking, pg_find_plans is nothing more than a simple set of functions
for storing JSON explain texts of plans that appear as pg_stat_plans entries
into a dedicated table, and subsequently parsing those plans to answer
interesting questions using SQL. However, pg_find_plans is a module that is
likely to make pg_stat_plans much more useful than it might otherwise be.
pg_find_plans is by no means feature complete or especially polished. The
author's ambitions for the tool are described under "limitations" below.

Sample queries
==============

Sample queries are available from the file samples.sql, which is distributed
with pg_find_plans in a subdirectory of pg_stat_plans. These give usage examples
that are likely to answer questions interesting to DBAs running production
PostgreSQL systems, including showing costs for plans involving particular
tables or indexes, or showing plan costs organized by the number of joins
involved in a query.

Installation
============

pg_find_plans has some dependencies above and beyond those of pg_stat_plans. This
is one reason why it is not required to use pg_find_plans with pg_stat_plans.
pg_find_plans requires:

* pg_stat_plans + PostgreSQL. pg_find_plans will work with any version of
  Postgres that is supported by pg_stat_plans.

* PL/PgSQL. Available by default on all Postgres versions that pg_stat_plans
  supports, so arguably its inclusion here is redundant.

* PL/Python, based on Python version 2.6+. Since the Python standard library
  JSON decoder was introduced in that version, it is unlikely that pg_find_plans
  will ever support an earlier version of Python. pg_find_plans should work with
  any version of Python 2 after 2.6, but the Python 3 language variant is not
  currently supported.

Although the module is entirely written in SQL, PL/PgSQL and PL/Python, there is
a PGXS Makefile supplied, which manages installation. If you are using a
packaged PostgreSQL build and have ``pg_config`` available (and in your OS
user's $PATH), the procedure is as follows::

  cd pg_find_plans
  make
  make install

You may need to refer to platform-specific notes on installation issues that
appear in the main pg_stat_plans README.

Setting up PostgreSQL
---------------------

pg_find_plans objects must be installed in every database that they are
required. pg_find_plans uses the PostgreSQL extension mechanism where available.
To install on PostgreSQL versions 9.1+, execute the following SQL command::

  mydb=# CREATE EXTENSION pg_find_plans;

Earlier releases (that lack the extension mechanism - in practice, this is
limited to version 9.0) must install the module by executing the SQL script
directly::

  psql mydb -f pg_find_plans.sql

Making sure that new plans are available to search through
----------------------------------------------------------

Finally, you'll need to figure out some way of maintaining the materialized
plans table, stored_plans, since Postgres has no built-in way of scheduling
arbitrary maintenance tasks. See "Suggested setup" below.

Objects made available by pg_stat_plans
=======================================

The pg_find_plans module provides numerous database objects. Example usage for
most of these objects is shown within samples.sql.

node_type enum
--------------

This enum is comprised of constants that exactly match node types within a
non-text format Postgres EXPLAIN (as of Postgres 9.2 - versions 9.0 and 9.1 of
Postgres have constants that are a strict subset of the 9.2 values).  The
following psql command will show all possible values::

 mydb=# \dT+ node_type

command_type enum
------------------------

See notes on contains_modifytable function below.

agg_strategy enum
----------------------------

See notes on contains_aggregate function below.

setop_strategy enum
------------------------

See notes on contains_setop function below.

join_type enum
--------------

See notes on join_count function below.

stored_plans table
------------------

The stored_plans table is where plans for pg_stat_plans entries may be
materialized to. Its definition is::

        Column       |       Type       | Modifiers
  -------------------+------------------+-----------
   planid            | oid              | not null
   userid            | oid              | not null
   dbid              | oid              | not null
   json_plan         | text             |
   last_startup_cost | double precision | not null
   last_total_cost   | double precision | not null
   error_seen        | text             |
  Indexes:
      "stored_plans_pkey" PRIMARY KEY, btree (planid, userid, dbid)

contains_node function
----------------------

This function is used to find plan nodes from a Postgres JSON-format explain
text. Its signature is::

  contains_node(json_plan text, node node_type default null,
                        relation regclass default null)
    returns boolean

The function takes a single plan, specified by json_plan, and a node type,
specified by node (which is an enum type with constants that correspond to the
names of nodes as they appear within non-text format explain plans). Optionally,
the user may specify a relation, which must refer to a relation (index or table)
within the current database. Note that it is possible to query for 'Index Scan'
and even 'Index Only Scan' against a table, and have a query show results for
any of that table's indexes. Note that node may be given as null, indicating
that all relation-scan nodes (plan leaf nodes) are of interest. This can be used
to return costs for plans that involve a given relation in *any* way. For
example::

  select *
  ...
  where contains_node(json_plan, relation := 'orders')

The boolean value returned by the function indicates if the JSON plan contains
node(s) that fit. If a relation was specified, the plan must contain the
relation in respect of the node in order to match. For example, if node was a
'Seq Scan', relation name might optionally be specified as 'mytable'. In that
case, the function would only return true if the plan contained a sequential
scan on 'mytable'. It would not be sufficient for the plan to contain just a
sequential scan and some other reference (such as a 'Bitmap Heap Scan') to the
relation 'mytable'.

contains_nodes function
-----------------------

This function is a convenience variant of contains_node, intended to be used
with multiple relations rather than just a single one. Its signature is::

  contains_nodes(json_plan text,
                        node node_type default null,
                        relations regclass[] default null)
    returns boolean

Again, if node is not specified, this indicates that all relation-scan nodes
(plan leaf nodes) are of interest. Relations may also be null, indicating that
all relations are of interest.

As with contains_node, the boolean value returned by contains_nodes indicates if
a match occurred.

contains_modifytable function
-----------------------------

This function is used to search for ModifyTable nodes based on Operation, and
potentially, affected relation. The functions signature is::

  contains_modifytable(json_plan text,
                          command command_type default null,
                          relation regclass default null)
    returns boolean

The boolean value returned by contains_modifytable indicates if a match
occurred.

"command" can be any one of 'Insert', 'Update' or 'Delete'. Alternatively, if
"command" is null, all ModifyTable nodes that operate on "relation" can be
returned. If "relation" is null, all ModifyTable nodes with commands/operations
that match "command" will cause the function to return true, regardless of what
relation they apply to.

contains_aggregate function
---------------------------

This function is used to search for aggregate nodes based on "Strategy". The
functions signature is::

  contains_aggregate(json_plan text, strategy agg_strategy)
    returns boolean

The strategy specified by "strategy" can be any one of 'Aggregate',
'GroupAggregate' and 'HashAggregate'. Those who are used to the text explain
format may be puzzled by this, regarding these various types of aggregates as
distinct node types, rather than strategies of a single node. In fact, this
scheme better reflects the actual structure of the code within the executor.

The boolean value returned by contains_aggregate indicates if a match occurred.

contains_setop function
-----------------------

This function is used to search for set operation nodes based on "Strategy". The
functions signature is::

  contains_setop(json_plan text, strategy setop_strategy)
    returns boolean

The strategy specified by "strategy" can be either 'Sorted' (typically seen when
the set operation must eliminate duplicates) or 'Hashed' (typically seen for
union all set operations).

Note that set operations are not guaranteed to produce a plan with a SetOp node.
For example, it is possible for the planner to produce an Append node.

The boolean value returned by contains_setop indicates if a match occurred.

join_count function
-------------------

join_count returns the number of joins within a JSON-format explain text. Its
signature is::

  join_count(json_plan text, count join_type default null)
    returns integer

The count_type argument limits joins counted to one of several types of join.
These are: 'Inner', 'Left', 'Full', 'Right', 'Semi', 'Anti' and 'Outer' (i.e.
any one of 'Left', 'Full', 'Right'). If count_type is null, all join types are
considered of interest. Note that if a particular join *strategy* type is of
interest (such as a 'Nested Loop' join), nodes of that type can be found with
the contains_node function. Naturally, all JSON plan parsing pg_find_plans
functions can be usefully combined within a query predicate.

materialize_plans function
--------------------------

This function aggregates JSON explain texts for each entry within
pg_stat_plans::

  materialize_plans(ignore_costs boolean default true)
    returns void

It lazily explains only those entries of pg_stat_plans that don't already have
an entry. It is the intended infrastructure through which DBAs can materialize
plans tracked by pg_stat_plans asynchronously. See notes in "Suggested setup"
below, including details of the "ignore_costs" argument. "ignore_costs" has the
function not consider differing costs (between materialized JSON plans and the
pg_stat_plans view) as a reason for re-EXPLAINing.

trim_stored_plans function
--------------------------

This function "trims" (i.e. deletes) entries within stored_plans that are no
longer present within pg_stat_plans, typically due to application churn and
pressure on pg_stat_plans' fixed-sized cache removing marginal entries/plans.
Its signature is::

  trim_stored_plans()
    returns void

Typically usage is described below, under "Suggested setup".

Suggested setup
===============

The need to maintain materialized plans within a dedicated table naturally
implies some overhead. However, since in practice the actual entries within
pg_stat_plans can be expected to be reasonably static, pg_find_plans' ability to
lazily explain only new entries that lack an explain text makes that overhead
likely to be quite modest in practice.

It is assumed that it is not absolutely critical that the latest plan texts are
always available. samples.sql contains a query that will show entries as yet
unprocessed by pg_find_plans. In order to maintain materialized JSON plan texts,
the following SQL should be executed at regular intervals, such as every 15
minutes::

  select materialize_plans();

This will only explain and store new plans that have not yet been processed. It
may also be useful to call the function in this manner less frequently, perhaps
once a day::

  select materialize_plans(ignore_costs:=false);

This will cause materialize_plans() to lazily skip existing entries if and only
if both the startup cost and total cost of each query, as stored within the
stored_plans materialization table, and the last startup and total costs
pg_stat_plans saw for a given query match exactly. In other words, this should
be done out of a concern for keeping the plan costs for each plan, as
represented in the stored JSON plan texts, consistent with their actual current
values as measured by the planner. It is quite possible for a plan's costs to
change, without the planner producing a substantively different plan, due to
alterations in planner cost constants (i.e. server settings), new statistics
becoming available after ``ANALYZE`` is run, and so on.

  `N.B.: This SQL needs to be called from each and every database of your
  PostgreSQL installation that is of interest. pg_find_plans assumes that only
  the current database is of interest, despite the fact that pg_stat_plans
  aggregates statistics for the entire installation.`

You may also wish to delete unused plans within the stored_plans table, by
calling trim_stored_plans() at regular intervals. It may make sense to do so
less often, perhaps at the same time as performing a materialize_plans() that
does not ignore startup_costs/total_cost as a reason to refresh plan texts.

Example crontab
---------------

To follow the advice above, the following crontab can be used::

  # Every 15 minutes do small job (though not between 4 and 5)
  */15 0-3,5-23 * * * psql -d mydb -f /path/to/materialize_ignore.sql
  # 4:15, 4:30, 4:45 - still not worried about costs + trimming
  15-45/15 4 * * * psql -d mydb -f /path/to/materialize_ignore.sql
  # At 4:00, perform extra work. These operations will block each other, so
  # we've avoiding doing an "ignore" and "no ignore + trim" materialize (or,
  # indeed, any type of materialize) at the same time. Note that
  # materialize_plans() acquires a NOWAIT table-level lock, and so
  # materialization requests cannot pile-up due to unusually slow planning.
  0 4 * * * psql -d mydb -f /path/to/materialize_no_ignore_trim.sql

The ability to manipulate the scheduler from an SQL interface may be desired.
The PgAgent utility offers this capability, providing scheduler functionality
that can be manipulated through Postgres (i.e. schedules are stored in regular
tables) on all supported platforms. PgAgent is available from:

http://www.pgadmin.org/download/pgagent.php

Limitations
===========

Unlike pg_stat_plans, pg_find_plans is an experimental tool, and expedient hack;
it relies on the stored query text within pg_stat_plans to reproduce the same
plan originally observed asynchronously, often some number of minutes later. In
general, the query text cannot reproduce the EXPLAIN with a high degree of
reliability, since the planner's preferences may have changed for various
reasons noted separately in the main pg_stat_plans README. We may easily
encounter a scenario in which we have no reasonable expectation of producing an
EXPLAIN ever working for some number of entries. While pg_stat_plan's
fingerprinting mechanism ensures that the JSON explain text is consistent with
the plan originally seen for a pg_stat_plans entry, there may be scenarios in
which an unreasonably high number of entries can never be explained. Most
obviously, pg_find_plans will never work with prepared queries, and all PLs use
prepared queries internally. For that reason, you may wish to only run
pg_find_plans when pg_stat_plans.track is set to 'top'.

Proof-of-concept
----------------

There is a fairly obvious basic, alternative approach that could be taken to
implement pg_find_plans' functionality in a much better-principled way: an
explain JSON text could be produced synchronously, within an ExecutorEnd() hook,
in the style of contrib/auto_explain. Since that process can be performed
directly on a queryDesc (the data structure that encapsulates everything that
the executor needs to execute a query, in particular, its plan), the fragility
of taking a query text as a proxy for a plan - the basic problem with
pg_find_plans - is entirely eliminated.

This is a non-trivial undertaking, though. contrib/auto_explain simply logs
slow-running plans. pg_stat_plans would have to store plans in way that readily
facilitated querying the plans, while imposing minimal overhead on statement
execution. For example, it would perhaps be necessary to allocate some fixed
amount of additional shared memory to store JSON explain texts, while avoiding
truncating these texts whenever possible, since truncation makes the texts
completely useless. Developing a mechanism for lazily and efficiently storing
those JSON texts in an arbitrarily sized area is surprisingly involved.

Future direction
----------------

In the future, this approach may be taken, and pg_find_plans may become largely
obsolete, at least as a mechanism for materializing JSON plans. It is far from
obvious how much demand there is for pg_find_plans type functionality at
present, and due to constraints on the author's time, these ideas are only ever
likely to be pursued due to strong demand from 2ndQuadrant support customers, or
the PostgreSQL community at large. pg_find_plans can be thought of as a
proof-of-concept for features that pg_stat_plans could directly offer polished,
mature versions of, given sufficient attention. It can also be thought of as a
springboard for ideas about how we can query the structure of the entirety of
your database's plans. If it is possible to ask arbitrary questions about the
structure of plans used in production, what questions are actually interesting?
How can we present that information in a way that is actually actionable or
immediately useful?
