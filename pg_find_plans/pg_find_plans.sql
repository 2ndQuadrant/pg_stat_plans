-- -----------------------------------------------------------------------------
-- pg_find_plans.sql
--
--    Object definitions for pg_find_plans extension module.
--
--	Copyright (c) 2012, 2ndQuadrant Ltd.
--
--
-- -----------------------------------------------------------------------------

create table stored_plans
(
	userid oid,
	dbid oid,
	planid oid,
	json_plan text,
	last_startup_cost double precision not null,
	last_total_cost double precision not null,
	error_seen text,
	primary key(userid, dbid, planid)
);
comment on table stored_plans is
'Table that plans are materialized to by pg_find_plans';

-- Taken from explain.c's ExplainNode() (for non-text output - the "sname"
-- variable assigned to in the switch statement):
create type node_type as enum
(
 'Result',
 'ModifyTable', -- Operation is either 'Insert', 'Update' or 'Delete'
 'Append',
 'Merge Append',
 'Recursive Union',
 'BitmapAnd',
 'BitmapOr',
 'Nested Loop',
 'Merge Join',
 'Hash Join',
 'Seq Scan',
 'Index Scan',
 'Index Only Scan',
 'Bitmap Index Scan',
 'Bitmap Heap Scan',
 'Tid Scan',
 'Subquery Scan',
 'Function Scan',
 'Values Scan',
 'CTE Scan',
 'WorkTable Scan',
 'Foreign Scan',
 'Materialize',
 'Sort',
 'Group',
 'Aggregate', -- see notes on agg_strategy below
 'WindowAgg',
 'Unique',
 'SetOp', -- see notes on setop_strategy below
 'LockRows',
 'Limit',
 'Hash'
);
comment on type node_type is
'Node type names for non-text explains';

create type command_type as enum
(
 'Insert',
 'Update',
 'Delete'
);
comment on type command_type is
E'Commands that are used by ModifyTable nodes. '
'Appears as \'Operation\' in explain output';

-- Note that the non-text format explains appear to lack some nodes seen in the
-- text format explains, like GroupAggregate and HashAggregate. This is because
-- this facet of the aggregate is represented as its strategy, which actually
-- more accurately represents the structure of the executor.
create type agg_strategy as enum
(
 'Plain',	-- "Aggregate"
 'Sorted',	-- "GroupAggregate"
 'Hashed'	-- "HashAggregate"
);
comment on type agg_strategy is
E'Aggregate \'Strategy\', as it appears within non-text explain output';

-- As with aggregates, what appear as distinct Set Operation nodes in text
-- explain output are broken out as strategies rather than appearing as proper
-- nodes in JSON format explains. Again, this more accurately reflects the
-- structure of the executor.
create type setop_strategy as enum
(
 'Sorted',	-- "SetOp"
 'Hashed'	-- "HashSetOp"
);
comment on type setop_strategy is
E'SetOp \'Strategy\', as it appears within non-text explain output';

create type join_type as enum
(
 'Inner',
 'Left',
 'Full',
 'Right',
 'Outer', -- i.e. any one of ('Left','Full','Right')

 -- Semijoins and anti-semijoins (as defined in relational theory) do not appear
 -- in the SQL JOIN syntax, but there are standard idioms for representing them
 -- (e.g., using EXISTS). The planner recognizes these cases and converts them
 -- to joins. They appear as such within explain output.
 'Semi',
 'Anti'
);
comment on type join_type is
E'\'Join type\' as it appears within non-text explain output';

/*
 * Notes on canonicalizing regclass textual representation (applies to functions
 * with regclass arguments):
 *
 * We use the regclass type to pass the names of relations to functions.
 * Ordinarily, this works fine, and serves to sanitise input. In fact, it even
 * appears to work fine in the event of a schema-qualified relation, even with a
 * schema-naive function that compares regclass to some textual unqualified
 * relation name field. This is because the I/O conversion routine for regclass
 * works hard to provide the most useful textual representation of what is
 * really just a pg_class OID. It strips any schema qualification that is
 * actually unnecessary. However, sometimes the qualification *is* necessary,
 * which could cause a search for that relation to choke - in general we need a
 * way to go pass off a regclass and get back a textual namespace (schema) name
 * (if any is needed) and a relation name, even though it may appear to be
 * unnecessary much of the time, due to there being no namespace, and due to
 * namespaces appearing to be stripped upon conversion.
 *
 * In short, it's nice that that happens, but we're not about to rely on it here
 * (except to do case conversion, where we rely on regclass to normalise for us,
 * so it doesn't matter is orders is given as Orders or ORDERS). That's why we
 * try and extract a bare relation name from a fully-qualified schema.relation.
 * (There is copy-pasta regex code to do this in each of the regclass functions;
 * we could refactor away this commonality, but I think that the boilerplate
 * PL/Python database code is uglier).
 *
 * One consequence of using regclass as an argument to functions is that we
 * cannot declare their volatility level as "immutable", which we'd be able to
 * do if text were used instead. This precludes using the functions in
 * expression indexes. This is deemed to be worth it.
 */

create function contains_node(json_plan text, node node_type default null,
										relation regclass default null)
	returns boolean
	AS
$fun$
import json
import re

if node is None and relation is None:
	plpy.error('Must specify node and/or relation')

if json_plan is None:
	return None

plan = json.loads(json_plan)['Plan']

# See "canonicalizing regclass" notes above
def extract_tab_ns(relname):
	m = re.search(r"(.*)\.(.*)", relname)
	schema = m.group(1) if m else None
	relation = m.group(2) if m else relname
	return (schema, relation)

# At least for now, do nothing with schema name, and just live with the
# abmiguity of looking for distinct relations with the same name in different
# schemas
conreln = None
if relation:
	nms = extract_tab_ns(relation)
	conreln = nms[1]

# Used for "node is null" (all relation scan nodes) case
rel_scan_nodes = ('Seq Scan', 'Index Scan', 'Index Only Scan',
					'Bitmap Index Scan', 'Bitmap Heap Scan', 'Tid Scan')

def find_node(plan, node, relname):
	if isinstance(plan, unicode):
		return False
	for k, v in plan.items():
		if k == 'Node Type' and v == node if node else v in rel_scan_nodes:
			for n in ('Relation Name', 'Index Name'):
				if n in plan and plan[n] == relname if relname else True:
					return True
		if isinstance(v, dict) and find_node(v, node, relname) or\
			isinstance(v, list) and any(find_node(i, node, relname) for i in v):
			return True
	return False

return find_node(plan, node, conreln)
$fun$
language plpythonu
stable cost 400;
comment on function contains_node(text, node_type, regclass) is
E'contains_node returns true if the JSON textual explain plan specified by '
'@json_plan contains a node type matches @node. If @relation is specified, '
'the plan must have the name of the relation associated with the node '
'specified by @node in order to return true (so the node had better be a '
'relation scan node, like a \'Seq Scan\' or \'Index Scan\' - however, we '
'don\'t enforce this for forward-compatibility reasons). Note that we\'re '
'more broad in our definition of what a relation is than the explain '
'format (though not the core system, since regclass covers any kind of '
'pg_class entry) - it is possible to ask for Index scans against either an '
'index or a table (i.e. any one of the table\'s indexes).';

create function contains_nodes(json_plan text, node node_type default null,
									relations regclass[] default null)
	returns boolean
	AS
$fun$
import json
import re

if node is None and relations is None or len(relations) == 0:
	plpy.error('Must specify node and/or relations')

if json_plan is None:
	return None

jplan = json.loads(json_plan)['Plan']

# See "canonicalizing regclass" notes above
def extract_tab_ns(relname):
	m = re.search(r"(.*)\.(.*)", relname)
	schema = m.group(1) if m else None
	relation = m.group(2) if m else relname
	return (schema, relation)

# Ensure conanical relnames stay null/none
if relations:
	conrelnames = []
	for r in relations:
		nms = extract_tab_ns(r)
		conrelnames.append(nms[1])
else:
	conrelnames = None

# Used for "node is null" (all relation scan nodes) case
rel_scan_nodes = ('Seq Scan', 'Index Scan', 'Index Only Scan',
					'Bitmap Index Scan', 'Bitmap Heap Scan', 'Tid Scan')

def find_nodes(plan, node, relns):
	if isinstance(plan, unicode):
		return False
	for k, v in plan.items():
		if k == 'Node Type' and v == node if node else v in rel_scan_nodes:
			# "Relation Name" just means "Table Name" in explains.
			# However, we accept both indexes and tables as relations.
			for n in ('Relation Name', 'Index Name'):
				if n in plan and plan[n] in relns if relns else True:
					return True
		if isinstance(v, dict) and find_nodes(v, node, relns) or\
			isinstance(v, list) and any(find_nodes(f, node, relns) for f in v):
			return True
	return False

return find_nodes(jplan, node, conrelnames)
$fun$
language plpythonu
stable cost 400;
comment on function contains_nodes(text, node_type, regclass[]) is
E'Convenience variant of contains_node, but takes array of regclass to match '
'multiple relations';

create function contains_modifytable(json_plan text,
										command command_type default null,
										relation regclass default null)
	returns boolean
	AS
$fun$
import json
import re

if command is None and relation is None:
	plpy.error('command and/or relation must be specified')

if json_plan is None:
	return None

plan = json.loads(json_plan)['Plan']

# See "canonicalizing regclass" notes above
def extract_tab_ns(relname):
	m = re.search(r"(.*)\.(.*)", relname)
	schema = m.group(1) if m else None
	relation = m.group(2) if m else relname
	return (schema, relation)

reln = None
if relation:
	nms = extract_tab_ns(relation)
	reln = nms[1]

def find_modtable(plan, relname):
	if isinstance(plan, unicode):
		return False
	for k, v in plan.items():
		if k == 'Node Type' and v == 'ModifyTable':
			if relname:
				# relname specified - must match it and command, if any
				if plan['Relation Name'] == relname and\
					command is None or plan['Operation'] == command:
					return True
			elif plan['Operation'] == command:
				# Just command to match, matched.
				return True
		if isinstance(v, dict) and find_modtable(v, relname) or\
			isinstance(v, list) and any(find_modtable(i, relname) for i in v):
			return True
	return False

return find_modtable(plan, reln)
$fun$
language plpythonu
stable cost 400;
comment on function contains_modifytable(text, command_type, regclass) is
E'contains_modifytable returns true if the JSON textual explain plan specified '
'by @json_plan contains a ModifyTable node that performs an operation specified '
'by @command (or all possible values of @command if @command is null). If '
'@relation is specified, the plan must also have the name of the relation '
'associated with the ModifyTable node in order to return true.';

create function contains_aggregate(json_plan text, strategy agg_strategy)
	returns boolean
	AS
$fun$
import json

if strategy is None:
	plpy.error('No strategy specified')

if json_plan is None:
	return None

plan = json.loads(json_plan)['Plan']

def find_agg(plan):
	if isinstance(plan, unicode):
		return False
	for k, v in plan.items():
		if k == 'Node Type' and v == 'Aggregate':
			# We have the node - just match the strategy.
			if plan['Strategy'] == strategy:
				return True
		if isinstance(v, dict) and find_agg(v) or\
			isinstance(v, list) and any(find_agg(i) for i in v):
			return True
	return False

return find_agg(plan)
$fun$
language plpythonu
immutable cost 400;
comment on function contains_aggregate(text, agg_strategy) is
E'contains_aggregate returns true iff the JSON textual explain plan specified '
'by @json_plan contains an Aggregate node with a strategy specified by '
'@strategy.';

create function contains_setop(json_plan text, strategy setop_strategy)
	returns boolean
	AS
$fun$
import json

if strategy is None:
	plpy.error('No strategy specified')

if json_plan is None:
	return None

plan = json.loads(json_plan)['Plan']

def find_setop(plan):
	if isinstance(plan, unicode):
		return False
	for k, v in plan.items():
		if k == 'Node Type' and v == 'SetOp':
			# We have the node - just match the strategy.
			if plan['Strategy'] == strategy:
				return True
		if isinstance(v, dict) and find_setop(v) or\
			isinstance(v, list) and any(find_setop(i) for i in v):
			return True
	return False

return find_setop(plan)
$fun$
language plpythonu
immutable cost 400;
comment on function contains_setop(text, setop_strategy) is
E'contains_setop returns true iff the JSON textual explain plan specified by '
'@json_plan contains a SetOp node with a strategy specified by @strategy.';

create function join_count(json_plan text, count join_type default null)
returns integer as
$fun$
import json

if json_plan is None:
	return None

jplan = json.loads(json_plan)['Plan']

def find_join_count(plan, jtype):
	njoins = 0
	if isinstance(plan, unicode):
		return njoins
	for k, v in plan.items():
		if k == 'Join Type':
			# Some type of join. Is it what we want?
			if jtype == 'Outer' and v in ('Left', 'Full', 'Right') or\
				jtype is None or v == jtype:
				njoins += 1
		elif isinstance(v, dict):
			njoins += find_joint_count(v, jtype)
		elif isinstance(v, list):
			njoins += sum(find_join_count(i, jtype) for i in v)
	return njoins

return find_join_count(jplan, count)
$fun$
language plpythonu
immutable cost 800;
comment on function join_count(text, join_type) is
E'Returns how many joins the text plan specified by @json_plan has.'
'@count may be null, indicating that any type of join is of interest.';

create function materialize_plans(ignore_costs boolean default true)
returns void
set pg_stat_plans.explain_format to 'json'
as
$fun$
-- This function might have been implemented in pl/python, but Postgres 9.0
-- doesn't support subtransactions in pl/python, which we'd need.
<<outer>>
declare
	nsucceed	int not null default 0;
	nfail		int not null default 0;
	cur			record;
	-- # skipped plans for final tally notice message stats. There is a minor
	-- race condition here (we assume no rowcount changes between now and later
	-- select), but that doesn't seem worth fixing if to do so implies
	-- materializing the entire view each call.
	nskplans	int	not null default (select count(*) from pg_stat_plans where
										from_our_database);
begin
	-- Acquire a heavy lock to prevent concurrent writes, avoiding the need for
	-- the usual upsert looping pattern. This function is expected to be the
	-- only writer to the stored_plans table, and a relatively irregular one at
	-- that, so this should be fine.
	--
	-- Lock with nowait mode, so if planning takes an inordinate amount of time,
	-- requests from a cronjob or something won't uselessly pile up, and the DBA
	-- will have a better chance of noticing something is wrong.
	lock @extschema@.stored_plans in exclusive mode nowait;

	-- Lazily only create records for those plans that have no stored plan text.
	-- We may also have to update existing records iff their costs have changed
	-- since last time and caller doesn't want to @ignore_costs.
	<<plan_upserter_loop>>
	for cur in select userid, dbid, planid, last_startup_cost, last_total_cost
		from pg_stat_plans psp
		where
		query_explainable -- deliberately omitted in count(*) query above
		and
		from_our_database
		and not exists(select 1 from @extschema@.stored_plans p where
					psp.userid = p.userid and
					psp.dbid = p.dbid and
					psp.planid = p.planid and
					-- If there is no plan stored, keep trying (in successive
					-- calls to this function) until there is.  This could be
					-- futile for some entries, if for example the plan relates
					-- to a "declare cursor" statement, or other "half utility,
					-- half optimizable" statements that pg_stat_plans balks at,
					-- but it likely isn't worth fixing those cases. We do avoid
					-- trying to explain entries that pg_stat_plans already
					-- knows to not currently be explainable, so continually
					-- trying to explain prepared query texts is avoided, for
					-- example.
					p.json_plan is not null and
					case
						when ignore_costs
							then true
						else
							-- cost differences may be a basis for re-explaining
							(psp.last_startup_cost = p.last_startup_cost and
								psp.last_total_cost = p.last_total_cost)
					end
					)
		loop
			<<upsert_plan>>
			declare
				plan	text default null;
				errm	text default null;
			begin
				-- processing existing entry, so one less entry that is
				-- considered skipped
				nskplans := nskplans - 1;

				<<trap_plan_errm>>
				begin
					plan := (select pg_stat_plans_explain(
														cur.planid,
														cur.userid,
														cur.dbid));
				exception when others then
					-- Some error during explaining - record this.
					errm := SQLERRM;
					nfail := nfail + 1;
				end trap_plan_errm;

				if plan is null and errm is null then
					-- No error given, and yet there is no plan text, so it must
					-- be that the plans (stored for entry and newly produced by
					-- explaining stored query text) don't match.
					--
					-- TODO: It would be nice to be able to extract the warning
					-- that pg_stat_plans_explain() probably raised in relation
					-- to this, to better facilitate later analysis. Hard to see
					-- a way of doing that without adding baggage to the
					-- pg_stat_plans_explain() interface, though.
					errm := 'query text produced distinct plan '
							'(old json_plan, if any, retained)';
					nfail := nfail + 1;
				else
					nsucceed := nsucceed + 1;
				end if;

				insert into @extschema@.stored_plans
										(
											userid,
											dbid,
											planid,
											last_startup_cost,
											last_total_cost,
											json_plan,
											error_seen
										)
										select
											cur.userid,
											cur.dbid,
											cur.planid,
											cur.last_startup_cost,
											cur.last_total_cost,
											plan,
											errm;
			exception
				when unique_violation then
					-- Update instead - there was a plan with the same pk but
					-- different costs.
					update @extschema@.stored_plans
						set json_plan = plan,
							last_startup_cost = cur.last_startup_cost,
							last_total_cost = cur.last_total_cost,
							error_seen = errm
							where
								userid = cur.userid
							and
								dbid = cur.dbid
							and
								planid = cur.planid;
			end upsert_plan;
	end loop plan_upserter_loop;

	<<report>>
	declare
		proc 	int		not null default (nsucceed + nfail);
		entryp	text 	not null default (select case proc when 1 then 'entry'
											else 'entries' end);
		entrys	text 	not null default (select case nskplans when 1 then 'entry'
											else 'entries' end);
	begin
		raise notice 'Processed % %, of which % produced the expected '
					'json plan, and % did not. % % skipped '
					'(perhaps due to non-explainability).', proc, entryp,
					nsucceed, nfail, nskplans, entrys;
	end report;
end outer;
$fun$
language plpgsql
volatile;
comment on function materialize_plans(boolean) is
E'Materialize plans into stored_plans table for later analysis. @ignore_costs '
'indicates if we care that last_costs now differ from value stored in '
'stored_plans table. If so, costs within plan have changed, which warrants '
're-explaining stored query text to see new costs (even though the entry does '
'of course still relate to the same plan).';

create function trim_stored_plans()
returns void as
$fun$
delete from @extschema@.stored_plans sp
where
not exists(select 1 from pg_stat_plans p where
					sp.userid = p.userid and sp.dbid = p.dbid and sp.planid = p.planid);
$fun$
language sql
volatile;
comment on function trim_stored_plans() is
E'Removes plans materialized to the stored_plans table that are no longer '
'present in pg_stat_plans hashtable';

-- These should be usable by public
grant select on stored_plans to public;
grant execute on function contains_node(text, node_type, regclass) to public;
grant execute on function contains_nodes(text, node_type, regclass[]) to public;
grant execute on function contains_modifytable(text, command_type, regclass) to public;
grant execute on function contains_aggregate(text, agg_strategy) to public;
grant execute on function contains_setop(text, setop_strategy) to public;
grant execute on function join_count(text, join_type) to public;
-- Don't want this to be available to public
revoke all on function materialize_plans(boolean) from public;
revoke all on function trim_stored_plans() from public;
