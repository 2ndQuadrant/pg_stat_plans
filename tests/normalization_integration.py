#!/usr/bin/env python

"""
Normalization integration tests for pg_stat_plans

This is a modified version of the tests that originally drove the development
of the query fingerprinting/normalization feature in Postgres 9.2's
pg_stat_statements.

I assume that the dellstore 2 dump has been restored to the postgres database
http://pgfoundry.org/forum/forum.php?forum_id=603

Any concurrently active session can be expected to break these regression
tests, as they rely on the number of tuples appearing in the pg_stat_plans view
not changing due to external factors.

Note that these tests are only for normalization (through plan fingerprinting).
We make no effort to vary plans between calls here, so the "planner regression"
stuff that might cause multiple entries for what is technically the same query
isn't being excercised.

Since we are testing the hashing of plans, it is necessary to run the tests
with a stock postgresql.conf. You should also run ANALYZE manually after
restoring the dellstore database. These tests are not so sophisticated that we
write them with a particular query plan in mind, but we should still apply a
consistent standard insofar as possible.
"""

import psycopg2
import sys

test_no = 1
failures = 0


def print_queries(conn):
    print "Queries that were found in pg_stat_plans: "
    cur = conn.cursor()
    cur.execute("select query from pg_stat_plans;")
    for i in cur:
        print i[0]


def print_query_trees(sql, equiv, cur):
    # Print both query's query trees
    import os
    global test_no
    cur.execute("select pg_stat_plans_pprint(%s)", (sql, ))
    treea = cur.fetchone()[0]
    cur.execute("select pg_stat_plans_pprint(%s)", (equiv, ))
    treeb = cur.fetchone()[0]
    sys.stderr.write("first:\n" + treea + "\n")
    sys.stderr.write("second:\n" + treeb + "\n")

    f_a = open("tree_test_" + str(test_no) + "_a", 'w')
    f_b = open("tree_test_" + str(test_no) + "_b", 'w')
    f_a.write(treea)
    f_b.write(treeb)
    f_a.close()
    f_b.close()
    os.system("diff -u " + f_a.name + " " + f_b.name + "> test" + str(test_no))


def verify_statement_equivalency(sql, equiv, conn, test_name=None, cleanup_sql=None):
    """
    Run both queries in isolation and verify that there is only a single tuple
    """

    global test_no
    global failures
    cur = conn.cursor()
    cur.execute("select pg_stat_plans_reset();")
    cur.execute(sql)
    if cleanup_sql is not None:
        cur.execute(cleanup_sql)

    cur.execute(equiv)
    if cleanup_sql is not None:
        cur.execute(cleanup_sql)

    cur.execute(
    # Exclude both pg_stat_plans_reset() calls and foreign key enforcement queries
    """select count(*) from pg_stat_plans
        where query not like '%pg_stat_plans%'
        and
        query not like '%OPERATOR(pg_catalog.=) $1 FOR SHARE OF x%'
        and
        query not like '%pg_catalog.pg_description%'
        and
        query not like '%pg_catalog.date_part%'
        {0};""".format(
        "" if cleanup_sql is None else "and query != '{0}'".format(cleanup_sql))
            )

    for i in cur:
        tuple_n = i[0]

    if tuple_n != 1:
        sys.stderr.write("""The SQL statements \n'{0}'\n and \n'{1}'\n do not
                appear to be equivalent!  Test {2} failed.\n""".format(sql,
                equiv, test_no if test_name is None else "'{0}'({1})".format(test_name, test_no)))
        failures += 1
        print_query_trees(sql, equiv, cur)
    else:
        print """The statements \n'{0}'\n and \n'{1}'\n are equivalent, as
        expected.  Test {2} passed.\n\n""".format(sql, equiv, test_no if test_name
        is None else "'{0}' ({1})".format(test_name, test_no))

    test_no += 1


def verify_statement_differs(sql, diff, conn, test_name=None, cleanup_sql=None):
    """
    Run both queries in isolation and verify that there are
    two tuples
    """

    global test_no
    global failures
    cur = conn.cursor()
    cur.execute("select pg_stat_plans_reset();")
    cur.execute(sql)
    if cleanup_sql is not None:
        cur.execute(cleanup_sql)
    cur.execute(diff)
    if cleanup_sql is not None:
        cur.execute(cleanup_sql)

    cur.execute(
    # Exclude both pg_stat_plans_reset() calls and foreign key enforcement queries
    """select count(*) from pg_stat_plans
        where query not like '%pg_stat_plans%'
        and
        query not like '%OPERATOR(pg_catalog.=) $1 FOR SHARE OF x%'
        and
        query not like '%pg_catalog.pg_description%'
        and
        query not like '%pg_catalog.date_part%'
        {0};""".format(
        "" if cleanup_sql is None else "and query != '{0}'".format(cleanup_sql))
            )
    for i in cur:
        tuple_n = i[0]

    if tuple_n != 2:
        sys.stderr.write("""The SQL statements \n'{0}'\n and \n'{1}'\n do not
                appear to be different!  Test {2} failed.\n""".format(sql, diff,
                test_no if test_name is None else "'{0}'({1})".format(test_name, test_no)))
        failures += 1
        print_query_trees(sql, diff, cur)

    print """The statements \n'{0}'\n and \n'{1}'\n are not equivalent, as expected.
        Test {2} passed.\n\n """.format(sql, diff, test_no if test_name is None else "'{0}' ({1})".format(test_name, test_no))
    test_no += 1


def test_assert(assertion, des, conn):
    global test_no
    global failures
    if not assertion:
        failures += 1
        sys.stderr.write("Assertion (\"{0}\")(test {1}) failed!\n\n".format(des, test_no))
    else:
        print """Assertion (\"{0}\") good. Test {1} passed.\n""".format(des, test_no)
    test_no += 1


def main():
    """
    Run all tests while tracking all statements (i.e. nested ones too, within
    functions). This is necessary for the test_sync_issues() test, but shouldn't
    otherwise matter. I suspect that it may usefully increase test coverage
    in some cases at some point in the code's development.
    """

    conn = psycopg2.connect("")
    cur = conn.cursor()
    cur.execute("set pg_stat_plans.track = 'all';")

    verify_statement_equivalency("select '5'::integer;", "select  '17'::integer;", conn)
    verify_statement_equivalency("select 1;", "select      5   ;", conn)
    verify_statement_equivalency("select 1::integer;", "select NULL::integer;", conn)
    verify_statement_equivalency("select 'foo'::text;", "select  'bar'::text;", conn)
    # Date constant normalization
    verify_statement_equivalency("select * from orders where orderdate = '2001-01-01';", "select * from orders where orderdate = '1960-01-01'", conn)
    verify_statement_equivalency("select '5'::integer;", "select  '17'::integer;", conn)
    # Test equivalency of cast syntaxes:
    verify_statement_equivalency("select '5'::integer;", "select integer '5'", conn)

    # We don't care about whitespace or differences in constants:
    verify_statement_equivalency(
    "select o.orderid from orders o join orderlines ol on o.orderid = ol.orderid     where    customerid        =  12345  ;",
    "select o.orderid from orders o join orderlines ol on o.orderid = ol.orderid where customerid = 6789;", conn)

    # using clause matters:
    verify_statement_differs(
    "select o.orderid from orders o join orders oo using (orderid);",
    "select o.orderid from orders o join orders oo using (customerid);", conn)

    # "select * from " and "select <enumerate columns> from " equivalency:
    verify_statement_equivalency("select * from orders", "select orderid, orderdate, customerid, netamount, tax, totalamount from orders;", conn)
    # The equivalency only holds if you happen to enumerate columns in the exact same order though:
    verify_statement_differs("select * from orders", "select orderid, orderdate, customerid, tax, netamount, totalamount from orders", conn)
    # columns must match:
    verify_statement_differs("select  customerid from orders", "select orderid from orders", conn)
    # We haven't really resolved the types of these values, so they're actually equivalent:
    verify_statement_equivalency("select 1;", "select 1000000000;", conn)
    # However, these are not:
    verify_statement_differs("select 1::integer", "select 1000000000::bigint", conn)
    # Types must match - types aren't resolved here, but they are partially resolved:
    verify_statement_differs("select 5", "select 'foo'", conn)
    # Join Qual differences matter:
    verify_statement_differs("select * from orders o join orderlines ol on o.orderid = ol.orderid;",
                "select * from orders o join orderlines ol on o.orderid = 5;", conn)
    # Although differences in Join Qual constants do not:
    verify_statement_equivalency("select * from orders o join orderlines ol on o.orderid = 66;",
                "select * from orders o join orderlines ol on o.orderid = 5;", conn)
    # Constants may vary here:
    verify_statement_equivalency("select orderid from orders where orderid = 5 and 1=1;",
                "select orderid from orders where orderid = 7 and 3=3;", conn)
    # Different logical operator means different query though:
    verify_statement_differs("select orderid from orders where orderid = 5 and 1=1;",
                "select orderid from orders where orderid = 7 or 3=3;", conn)

    # don't mistake the same column number (MyVar->varno) from different tables:
    verify_statement_differs("select a.orderid    from orders a join orderlines b on a.orderid = b.orderlineid",
                "select b.orderlineid from orders a join orderlines b on a.orderid = b.orderlineid", conn)

    # Boolean Test node:
    verify_statement_differs(
    "select orderid from orders where orderid = 5 and (1=1) is true;",
    "select orderid from orders where orderid = 5 and (1=1) is not true;",
    conn)

    verify_statement_equivalency(
        "values(1, 2, 3);",
        "values(4, 5, 6);", conn)

    verify_statement_differs(
        "values(1, 2, 3);",
        "values(4, 5, 6, 7);", conn)

    verify_statement_equivalency(
        "select * from (values(1, 2, 3)) as v;",
        "select * from (values(4, 5, 6)) as v;", conn)

    verify_statement_differs(
        "select * from (values(1, 2, 3)) as v;",
        "select * from (values(4, 5, 6, 7)) as v;", conn)

    # Coalesce
    verify_statement_equivalency("select coalesce(orderid, 3, 2) from orders where orderid = 5 and 1=1;",
                    "select coalesce(orderid, 5, 5) from orders where orderid = 7 and 3=3;", conn)

    # Observe what we can do with noise words (no "outer" in later statement, plus we use AS in the second query):
    verify_statement_equivalency("select * from orders o left outer join orderlines ol on o.orderid = ol.orderid;",
                    "select * from orders AS o left join orderlines AS ol on o.orderid = ol.orderid;", conn)

    # Join order in statement matters:
    verify_statement_differs("select * from orderlines ol join orders o on o.orderid = ol.orderid;",
                    "select * from orders o join orderlines ol on o.orderid = ol.orderid;", conn)

    # Join strength/type matters:
    verify_statement_differs("select * from orderlines ol inner join orders o on o.orderid = ol.orderid;",
                    "select * from orderlines ol left outer join orders o on o.orderid = ol.orderid;", conn)

    verify_statement_equivalency(
    "select 1 from orders where 1=55 and 'foo' = 'fi' or 'foo' = upper(lower(upper(initcap(initcap(lower(orderid::text))))));",
    "select 1 from orders where 1=2 and 'feew' = 'fi' or 'bar' = upper(lower(upper(initcap(initcap(lower(orderid::text))))));",
                conn)

    verify_statement_differs(
    "select 1 from orders where 1=55 and 'foo' = 'fi' or 'foo' = upper(lower(upper(initcap(upper  (lower(orderid::text))))));",
    "select 1 from orders where 1=2 and 'feew' = 'fi' or 'bar' = upper(lower(upper(initcap(initcap(lower(orderid::text))))));",
                conn)

    verify_statement_differs(
    "select 1 from orders where 1=55 and 'foo' = 'fi' or upper(lower(upper(initcap(upper(lower(orderid::text)))))) is null;",
    "select 1 from orders where 1=55 and 'foo' = 'fi' or upper(lower(upper(initcap(upper(lower(orderid::text)))))) is not null;",
                conn)

    verify_statement_differs(
    "select 1 from orders where 1=55 and 'foo' = 'fi' or     upper(lower(upper(initcap(upper(lower(orderid::text)))))) is null;",
    "select 1 from orders where 1=55 and 'foo' = 'fi' or not upper(lower(upper(initcap(upper(lower(orderid::text)))))) is null;",
                conn)

    # Nested BoolExpr is a differentiator:
    verify_statement_differs(
    "select 1 from orders where 1=55 and 'foo' = 'fi' or  'foo' = upper(lower(upper(initcap(upper (lower(orderid::text))))));",
    "select 1 from orders where 1=55 and 'foo' = 'fi' and 'foo' = upper(lower(upper(initcap(upper (lower(orderid::text))))));",
                conn)

    # For aggregates too
    verify_statement_differs(
    "select array_agg(lower(upper(initcap(initcap(lower(orderid::text)))))) from orders;",
    "select array_agg(lower(upper(lower(initcap(lower(orderid::text)))))) from orders;",
                conn)

    verify_statement_equivalency(
    "select array_agg(lower(upper(lower(initcap(lower(orderid::text)))))) from orders;",
    "select array_agg(lower(upper(lower(initcap(lower(orderid::text)))))) from orders;",
                conn)
    # Row-wise comparison
    verify_statement_differs(
    "select (1, 2) < (3, 4);",
    "select ('a', 'b') < ('c', 'd');",
                conn)
    verify_statement_differs(
    "select (1, 2, 3) < (3, 4, 5);",
    "select (1, 2) < (3, 4);",
                conn)
    verify_statement_equivalency(
    "select (1, 2, 3) < (3, 4, 5);",
    "select (3, 4, 5) < (1, 2, 3);",
                conn)

    # Use lots of different operators:
    verify_statement_differs(
    "select 1 < orderid from orders;",
    "select 1 <= orderid from orders;",
                conn)

    verify_statement_differs(
    "select ARRAY[1,2,3]::integer[] && array_agg(orderid) from orders;",
    "select ARRAY[1,2,3]::integer[] <@ array_agg(orderid) from orders;",
                conn)
    # array subscripting operations
    verify_statement_equivalency(
    "select (array_agg(lower(upper(lower(initcap(lower(orderid::text)))))))[5:5] from orders;",
    "select (array_agg(lower(upper(lower(initcap(lower(orderid::text)))))))[6:6] from orders;",
                conn)
    verify_statement_differs(
    "select (array_agg(lower(upper(lower(initcap(lower(orderid::text)))))))[5:5] from orders;",
    "select (array_agg(lower(upper(lower(initcap(lower(orderid::text)))))))[6] from orders;",
                conn)

    cur = conn.cursor()
    cur.execute("CREATE TEMP TABLE arrtest1 (i int[], t text[]);")

    # nullif, represented as a distinct node but actually just a typedef
    verify_statement_differs(
    "select *, (select customerid from orders limit 1), nullif(5,10) from orderlines ol join orders o on o.orderid = ol.orderid;",
    "select *, (select customerid from orders limit 1), nullif('a','b') from orderlines ol join orders o on o.orderid = ol.orderid;",
    conn)

    verify_statement_equivalency(
    "select *, (select customerid from orders limit 1), nullif(5,10) from orderlines ol join orders o on o.orderid = ol.orderid;",
    "select *, (select customerid from orders limit 1), nullif(10,15) from orderlines ol join orders o on o.orderid = ol.orderid;",
    conn)

    # XML Stuff
    verify_statement_differs(
    """
    select xmlagg
    (
        xmlelement
        (   name database,
            xmlattributes (d.datname as "name"),
            xmlforest(
                pg_database_size(d.datname) as size,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_fetched,
                tup_returned,
                tup_inserted,
                tup_updated,
                tup_deleted
                )
        )
    )
    from        pg_stat_database d
    right join  pg_database
    on      d.datname = pg_database.datname
    where       not datistemplate;
    """,
    """
    select xmlagg
    (
        xmlelement
        (   name database,
            xmlattributes (d.datname as "name"),
            xmlforest(
                pg_database_size(d.datname) as size,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_fetched,
                tup_returned,
                tup_updated,
                tup_deleted
                )
        )
    )
    from        pg_stat_database d
    right join  pg_database
    on      d.datname = pg_database.datname
    where       not datistemplate;
    """,
    conn)

    verify_statement_differs(
    """
    select xmlagg
    (
        xmlelement
        (   name database,
            xmlattributes (d.blks_hit as "name"),
            xmlforest(
                pg_database_size(d.datname) as size
                )
        )
    )
    from        pg_stat_database d
    right join  pg_database
    on      d.datname = pg_database.datname
    where       not datistemplate;
    """,
    """
    select xmlagg
    (
        xmlelement
        (   name database,
            xmlattributes (d.blks_read as "name"),
            xmlforest(
                pg_database_size(d.datname) as size
                )
        )
    )
    from        pg_stat_database d
    right join  pg_database
    on      d.datname = pg_database.datname
    where       not datistemplate;
    """,
    conn)

    # subqueries
    # in select list
    verify_statement_differs("select *, (select customerid from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;",
            "select *, (select orderid    from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

    # select list nested subselection - inner most types differ
    verify_statement_differs(
    "select *, (select (select 1::integer from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;",
    "select *, (select (select 1::bigint  from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;", conn)
    # This time, they're the same
    verify_statement_equivalency(
    "select *, (select (select 1::integer from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;",
    "select *, (select (select 1::integer from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

    # in from - this particular set of queries entail recursive JoinExprNode() calls
    verify_statement_differs(
    "select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid a from orders) as t on ol.orderid=t.a;",
    "select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid, customerid a from orders) as t on ol.orderid = t.a;", conn)
    # another in from - entails recursive JoinExprNode() call
    verify_statement_equivalency(
    "select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid a from orders where orderid = 77) as t on ol.orderid=t.a;",
    "select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid b from orders where orderid = 5) as t on ol.orderid = t.b;", conn)
    # Even though these two queries will result in the same plan, they are not yet equivalent - they are not
    # semantically equivalent, as least by our standard. We could probably figure out a way of having the
    # equivalency recognized, but I highly doubt it's worth bothering, since recognizing most kinds of semantic
    # equivalence is generally more of a neat consequence of our implementation than a practical feature:
    verify_statement_differs("select * from orderlines ol inner join orders o on o.orderid = ol.orderid;",
                "select * from orderlines ol, orders o where o.orderid = ol.orderid;", conn)

    # Aggregate group by normalisation:
    verify_statement_differs("select count(*) from orders o group by orderid;",
                "select count(*) from orders o group by customerid;", conn)

    # Which aggregate was called matters
    verify_statement_differs("select sum(customerid) from orders o group by orderid;",
                "select avg(customerid) from orders o group by orderid;", conn)

    # As does the order of aggregates
    verify_statement_differs("select sum(customerid), avg(customerid) from orders o group by orderid;",
                "select avg(customerid), sum(customerid) from orders o group by orderid;", conn)

    # As does the having clause
    verify_statement_equivalency(
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 50;",
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 1000;",
    conn)

    # operator in having clause matters
    verify_statement_differs(
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100;",
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) = 100;",
    conn)

    # as does datatype
    verify_statement_differs(
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100;",
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100::bigint;",
    conn)
    verify_statement_equivalency(
    "select avg(customerid), sum(customerid::bigint) from orders o group by orderid having sum(customerid::bigint) > 100;",
    "select avg(customerid), sum(customerid::bigint) from orders o group by orderid having sum(customerid::bigint) > 15450;",
    conn)
    # Having Qual BoolExpr
    verify_statement_differs(
    "select avg(customerid), sum(customerid::bigint) from orders o group by orderid having not sum(customerid::bigint) > 100;",
    "select avg(customerid), sum(customerid::bigint) from orders o group by orderid having     sum(customerid::bigint) > 100;",
    conn)
    verify_statement_differs(
    "select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100;",
    "select avg(customerid), sum(customerid::bigint) from orders o group by orderid having sum(customerid::bigint) > 100;",
    conn)

    # columns order in "order by" matters:
    verify_statement_differs(
    "select * from orders o join orderlines ol on o.orderid = ol.orderid order by o.orderdate, o.orderid, customerid, netamount, totalamount, tax ;",
    "select * from orders o join orderlines ol on o.orderid = ol.orderid order by o.orderdate, o.orderid, customerid, netamount, tax, totalamount;", conn)

    # distinct on is a differentiator:
    verify_statement_differs("select distinct on(customerid) customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
                 "select distinct on(o.orderid)    customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)
    # both the "on" value and the absence or presence of the clause:
    verify_statement_differs("select distinct on(customerid) customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
                 "select             customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

    # select "all" is generally just a noise word:
    verify_statement_equivalency("select all customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
                     "select                 customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)
    # Updates are similarly normalised, and their internal representations shares much with select statements
    verify_statement_equivalency("update orders set orderdate='2011-01-06' where orderid=3;",
                     "update orders set orderdate='1992-01-06' where orderid     =     10;", conn)
    # updates with different columns are not equivalent:
    verify_statement_differs("update orders set orderdate='2011-01-06' where orderid = 3;",
                 "update orders set orderdate='1992-01-06' where customerid     =     10;", conn)

    # default within update statement
    verify_statement_equivalency(
    "update products set special=default where prod_id = 7;",
    "update products set special=default where prod_id = 10;",
    conn)

    verify_statement_equivalency(
    "insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6);",
    "insert into products(category, title, actor, price, special, common_prod_id) values (6,5,4,3,2,1);",
    conn)

    verify_statement_differs(
    "insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6), (1,2,3,4,5,6);",
    "insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6);",
    conn)

    # select into
    verify_statement_equivalency(
    "select * into orders_recent FROM orders WHERE orderdate >= '2002-01-01';",
    "select * into orders_recent FROM orders WHERE orderdate >= '2010-01-01';",
    cleanup_sql="drop table if exists orders_recent;", conn=conn)

    verify_statement_differs(
    "select * into orders_recent FROM orders WHERE orderdate >= '2002-01-01';",
    "select * into orders_recent FROM orders WHERE orderdate >  '2002-01-01';",
    cleanup_sql="drop table if exists orders_recent;", conn=conn)

    verify_statement_differs(
    "select orderdate into orders_recent FROM orders WHERE orderdate > '2002-01-01';",
    "select orderid   into orders_recent FROM orders WHERE orderdate > '2002-01-01';",
    cleanup_sql="drop table if exists orders_recent;", conn=conn)

    # CTE
    verify_statement_differs(
    "with a as (select customerid from orders ) select * from a",
    "with a as (select orderid from orders ) select * from a",
    conn)

    # set operation normalization occurs by walking the query tree recursively.
    verify_statement_differs("select orderid from orders union all select customerid from orders", "select customerid from orders union all select orderid from orders", conn)
    verify_statement_equivalency(
    "select 1, 2, 3 except select orderid, 6, 7 from orders",
    "select 4, 5, 6 except select orderid, 55, 33 from orders",
    conn)

    verify_statement_differs(
    "select orderid, 6       from orders union select 1,2 ",
    "select 55,      orderid from orders union select 4,5 ",
    conn,
    "column order differs (left child)")

    verify_statement_differs(
    "select 1, 2 union select orderid,   6       from orders",
    "select 4, 5 union select 55,        orderid from orderlines",
    conn,
    "column order differs (right child easy)")

    verify_statement_differs(
    "select 1, 2 union select orderid,   6       from orders",
    "select 4, 5 union select 55,        orderid from orders",
    conn,
    "column order differs (right child)")

    # union != union all
    verify_statement_differs(
    "select customerid from orders union all select customerid from orders",
    "select customerid from orders union     select customerid from orders",
    conn, "union != union all")

    verify_statement_equivalency(
                    """
                    select 1,2,3
                    union all
                    select 5,6,7 from orders
                    except
                    select orderid, 934, 194 from orderlines
                    intersect
                    select 66,67,68;
                    """,
                    """
                    select 535,8437,366
                    union all
                    select 1,1,1 from orders
                    except
                    select orderid, 73737, 4235 from orderlines
                    intersect
                    select 5432, 6667,6337;
                    """,
                    conn)

    verify_statement_differs(
    "select orderid    from orders union all select customerid from orders",
    "select customerid from orders union all select orderid    from orders",
    conn)

    verify_statement_differs(
    "select 1::integer union all select orderid from orders union all select customerid from orders",
    "select customerid from orders union all select orderid from orders", conn)

    # My attempt to isolate the set operation problem.

    # test passes if there is one less select,
    # even if it'si an int, or if there's
    # one more "select 1"
    verify_statement_differs(
                """
                select 1
                union
                select (current_date - orderdate + 5 + 6 + 7 + 8) from orderlines
                union
                select 1;
                """,
                """
                select 1
                union
                select orderid from orders
                union
                select 1;
                """,
                conn, "isolate set operations problem")

    # Same as above, but the one table is different
    verify_statement_differs(
                    """
                    select 1,2,3
                    union all
                    select 5,6,7 from orders
                    except
                    select orderid, 934, 194 from orderlines
                    intersect
                    select 66,67,68;
                    """,
                    """
                    select 535,8437,366
                    union all
                    select 1,1,1 from orders
                    except
                    select orderid, 73737, 4235 from orders
                    intersect
                    select 5432, 6667,6337;
                    """,
                    conn, "one table is different")

    # Same as original, but I've switched a column reference with a constant in
    # the second query
    verify_statement_differs(
                    """
                    select 1,2,3
                    union all
                    select 5,6,7 from orders
                    except
                    select orderid, 934, 194 from orderlines
                    intersect
                    select 66,67,68
                    """,
                    """
                    select 535,8437,366
                    union all
                    select 1,1,1 from orders
                    except
                    select 73737, orderid, 4235 from orderlines
                    intersect
                    select 5432, 6667,6337
                    """,
                    conn)

    # The left node in the set operation tree matches for before and after; we should still catch that
    verify_statement_differs(
                    """
                        select orderid from orders
                        union all
                        select customerid from orders
                        union all
                        select customerid from orders
                    """,
                    """
                        select orderid from orders
                        union all
                        select orderid from orders
                        union all
                        select customerid from orders
                    """, conn)

    verify_statement_differs(
                    """
                        select orderid, customerid from orders
                        union
                        select orderid, customerid from orders
                        order by orderid
                    """,
                    """
                        select orderid, customerid from orders
                        union
                        select orderid, customerid from orders
                        order by customerid
                    """, conn)

    verify_statement_differs(
    """
    with recursive j(n) AS (
    values (1)
    union all
    select n + 1 from j where n < 100
    )
    select avg(n) from j;
    """,
    """
    with recursive j(n) AS (
    values (1)
    union all
    select 1 from orders
    )
    select avg(n) from j;
    """,
    conn)

    # Excercise some less frequently used Expr nodes
    verify_statement_equivalency(
                    """
                        select
                        case orderid
                        when 0 then 'zero'
                        when 1 then 'one'
                        else 'some other number' end
                        from orders
                    """,
                    """ select
                        case orderid
                        when 5 then 'five'
                        when 6 then 'six'
                        else 'some other number' end
                        from orders

                    """, conn)

    verify_statement_differs(
                    """
                        select
                        case orderid
                        when 0 then 'zero'
                        else 'some other number' end
                        from orders
                    """,
                    """ select
                        case orderid
                        when 5 then 'five'
                        when 6 then 'six'
                        else 'some other number' end
                        from orders

                    """, conn)

    # Counter-intuitively, these two statements are equivalent...
    verify_statement_equivalency(
                    """
                        select
                        case when orderid = 0
                        then 'zero'
                        when orderid = 1
                        then 'one'
                        else 'other number' end
                        from orders
                    """,
                    """
                        select
                        case when orderid = 0
                        then 'zero'
                        when orderid = 1
                        then 'one' end
                        from orders
                    """, conn, "Case when test")

    # ...this is because no else clause is actually equivalent to "else NULL".

    verify_statement_equivalency(
                    """
                        select
                        case when orderid = 0
                        then 'zero'
                        when orderid = 1
                        then 'one'
                        else 'other number' end
                        from orders
                    """,
                    """
                        select
                        case when orderid = 5
                        then 'five'
                        when orderid = 6
                        then 'six'
                        else 'some other number' end
                        from orders
                    """, conn, "second case when test")

    verify_statement_differs("select min(orderid) from orders", "select max(orderid) from orders", conn, "min not max check")

    # This should match - small OID references like this should not cause these
    # two plans to be sunstantively different:
    #
    # Due to a "Heisenberg effect", the following test does not appear to pass,
    # precisely because it is instrumented. Fix this some day.
    #verify_statement_equivalency(
    #"SELECT *, query, pg_stat_plans_explain(planid, userid, dbid) AS explain FROM pg_stat_plans WHERE planid = 3489937752 AND userid = 16395 AND dbid = 165;",
    #"SELECT *, query, pg_stat_plans_explain(planid, userid, dbid) AS explain FROM pg_stat_plans WHERE planid = 3489937752 AND userid = 16395 AND dbid = 1655;",
    #conn, "OID references")

    # The parser uses a dedicated Expr node to handle greatest()/least()
    verify_statement_differs("select greatest(1,2,3) from orders", "select least(1,2,3) from orders", conn, "greatest/least differ check")

    # Window functions
    verify_statement_differs(
    "select sum(netamount) over () from orders",
    "select sum(tax) over () from orders",
    conn, "window function column check")

    verify_statement_differs(
    "select sum(netamount) over (partition by customerid) from orders",
    "select sum(netamount) over (partition by orderdate) from orders",
    conn, "window function over column differs check")

    verify_statement_differs(
    "select sum(netamount) over (partition by customerid order by orderid) from orders",
    "select sum(netamount) over (partition by customerid order by tax) from orders",
    conn, "window function over column differs check")

    # This test fails on 9.0. There are actually no differences in each query's
    # plan trees on that version though, so there's nothing I can do about it.
    if conn.server_version >= 901000:
        verify_statement_differs(
            """
                SELECT c.oid AS relid,
                    n.nspname AS schemaname,
                    c.relname
                FROM pg_class c
                LEFT JOIN pg_index i ON c.oid = i.indrelid
                LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
                LEFT JOIN pg_class x ON t.reltoastidxid = x.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = ANY (ARRAY['r'::"char", 't'::"char"])
                GROUP BY c.oid, n.nspname, c.relname, t.oid, x.oid;
            """,
            """
                SELECT c.oid AS relid,
                    n.nspname AS schemaname,
                    c.relname
                FROM pg_class c
                LEFT JOIN pg_index i ON c.oid = i.indrelid
                LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
                LEFT JOIN pg_class x ON t.reltoastidxid = x.oid
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = ANY (ARRAY['r'::"char", 't'::"char", 'v'::"char"])
                GROUP BY c.oid, n.nspname, c.relname, t.oid, x.oid;
            """, conn, "number of ARRAY elements varies in ANY()  in where clause")

    # problematic query
    verify_statement_equivalency(
    """
    SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
      pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, c2.reltablespace
    FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
      LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
    WHERE c.oid = i.indrelid AND i.indexrelid = c2.oid
    ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
    """,
    """
    SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
      pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, c2.reltablespace
    FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
      LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','d','x'))
    WHERE c.oid = i.indrelid AND i.indexrelid = c2.oid
    ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
    """,
    conn)

    verify_statement_differs(
    "select coalesce(orderid) from orders;",
    "select sum(orderid) from orders;",
    conn, "Don't confuse functions/function like nodes")

    # Domains
    cur = conn.cursor()
    cur.execute("drop domain if exists dtop; create domain dtop text check (substring(VALUE, 2, 1) = '1');")
    cur.execute("drop domain if exists dnotnull; create domain dnotnull integer;")
    conn.commit()

    # row types
    cur = conn.cursor()
    cur.execute("drop type if exists quad; drop type if exists complex; create type complex as (r float8, i float8); create type quad as (c1 complex, c2 complex);create temp table quadtable(f1 int, q quad);")
    conn.commit()

    cur = conn.cursor()

    cur.execute(
    """
    create or replace function dfunc(a anyelement, b anyelement = null, flag bool = true)
    returns anyelement as $$
    select case when $3 then $1 else $2 end;
    $$ language sql;
    """)
    conn.commit()

    verify_statement_equivalency(
    """
    SELECT p1.amoplefttype, p1.amoprighttype, p2.amoprighttype
    FROM pg_amop AS p1, pg_amop AS p2
    WHERE p1.amopfamily = p2.amopfamily AND
        p1.amoprighttype = p2.amoplefttype AND
        p1.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
        p2.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
        p1.amopstrategy = 3 AND p2.amopstrategy = 3 AND
        p1.amoplefttype != p1.amoprighttype AND
        p2.amoplefttype != p2.amoprighttype AND
        NOT EXISTS(SELECT 1 FROM pg_amop p3 WHERE
                     p3.amopfamily = p1.amopfamily AND
                     p3.amoplefttype = p1.amoplefttype AND
                     p3.amoprighttype = p2.amoprighttype AND
                     p3.amopstrategy = 3);
    """,
    """
    SELECT p1.amoplefttype, p1.amoprighttype, p2.amoprighttype
    FROM pg_amop AS p1, pg_amop AS p2
    WHERE p1.amopfamily = p2.amopfamily AND
        p1.amoprighttype = p2.amoplefttype AND
        p1.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
        p2.amopmethod = (SELECT oid FROM pg_am WHERE amname = 'btree') AND
        p1.amopstrategy = 3 AND p2.amopstrategy = 3 AND
        p1.amoplefttype != p1.amoprighttype AND
        p2.amoplefttype != p2.amoprighttype AND
        NOT EXISTS(SELECT 1 FROM pg_amop p3 WHERE
                     p3.amopfamily = p1.amopfamily AND
                     p3.amoplefttype = p1.amoplefttype AND
                     p3.amoprighttype = p2.amoprighttype AND
                     p3.amopstrategy = 2);
    """,
    conn)

    # Constant is past track_activity_query_size
    verify_statement_equivalency(
    "select 1," + (" " * 2045) + "55;",
    "select 4," + (" " * 3088) + "3456;",
    conn, "late constant")

    global failures
    sys.stderr.write("Failures: {0} out of {1} tests.\n".format(failures, test_no - 1))

if __name__ == "__main__":
    main()
