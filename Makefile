# pg_stat_plans/Makefile

MODULE_big = pg_stat_plans
OBJS = pg_stat_plans.o

EXTENSION = pg_stat_plans
DATA = pg_stat_plans--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_stat_plans
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
