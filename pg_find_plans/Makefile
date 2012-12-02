EXTENSION    = pg_find_plans
EXTVERSION   = 1.0
EXTCOMMENT   = Tools to support searching pg_stat_plans
MODULEDIR    = pg_find_plans
REQUIRES     = pg_stat_plans

DOCS         = README.rst

PG_CONFIG    = pg_config
BUILD_EXTENSION = $(shell $(PG_CONFIG) --version | grep -qE "8\.|9\.0" && echo no || echo yes)

# Default (no Extension support)
DATA         = $(EXTENSION).sql uninstall_$(EXTENSION).sql
REGRESS      = $(EXTENSION)
pgext_files   := $(DOCS) $(DATA)

# Needed for building with extension support:
ifeq ($(BUILD_EXTENSION),yes)
all: $(EXTENSION)--$(EXTVERSION).sql $(EXTENSION)--unpackaged--$(EXTVERSION).sql

# Copy extension.sql to extension--version.sql:
$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
			cp $< $@

# Builds extension--unpackaged--version.sql from uninstall_extension.sql:
$(EXTENSION)--unpackaged--$(EXTVERSION).sql: uninstall_$(EXTENSION).sql
			sed 's/DROP /ALTER EXTENSION $(EXTENSION) ADD /' $< > $@

# Builds extension--unpackaged--version.sql from uninstall_extension.sql:
$(EXTENSION).control: $(EXTENSION).control.in
			sed 's/EXTVERSION/$(EXTVERSION)/;s/EXTENSION/$(EXTENSION)/;s/EXTCOMMENT/$(EXTCOMMENT)/;s/REQUIRES/$(REQUIRES)/' $< > $@

DATA        = $(EXTENSION)--unpackaged--$(EXTVERSION).sql $(EXTENSION)--$(EXTVERSION).sql
REGRESS     = $(EXTENSION).ext
EXTRA_CLEAN = $(DATA) $(EXTENSION).control
pgext_files  := $(DOCS)
endif

# Workaround for the lack of good VPATH support in PGXS for extension/contrib:
ifdef VPATH
pgext_files_build:= $(addprefix $(CURDIR)/, $(pgext_files))
pgext_reg_files  := $(addprefix $(CURDIR)/sql/, $(notdir $(wildcard $(VPATH)/sql/*.sql)))
pgext_reg_exp    := $(addprefix $(CURDIR)/expected/, $(notdir $(wildcard $(VPATH)/expected/*.out)))
all: $(pgext_files_build) $(pgext_reg_files) $(pgext_reg_exp)
$(pgext_files_build): $(CURDIR)/%: $(VPATH)/%
	cp $< $@
$(pgext_reg_files): $(CURDIR)/sql/%: $(VPATH)/sql/%
	mkdir -p $(dir $(pgext_reg_files))
	cp $< $@
$(pgext_reg_exp): $(CURDIR)/expected/%: $(VPATH)/expected/%
	mkdir -p $(dir $(pgext_reg_exp))
	cp $< $@
endif # VPATH

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
