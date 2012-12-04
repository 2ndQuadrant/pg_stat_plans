-- -----------------------------------------------------------------------------
-- uninstall_pg_find_plans.sql
--
--    Uninstall pg_find_plans extension module.
--
--	Copyright (c) 2012, 2ndQuadrant Ltd.
--
--
-- -----------------------------------------------------------------------------

drop function trim_stored_plans();
drop function materialize_plans(boolean);
drop function join_count(text, join_type);
drop function contains_setop(text, setop_strategy, regclass);
drop function contains_aggregate(text, agg_strategy, regclass);
drop function contains_modifytable(text, command_type, regclass);
drop function contains_nodes(text, node_type, regclass[]);
drop function contains_node(text, node_type, regclass);
drop type join_type;
drop type setop_strategy;
drop type agg_strategy;
drop type command_type;
drop type node_type;
drop table stores_plans;
