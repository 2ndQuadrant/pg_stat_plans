EXTENSION    = pg_stat_plans
EXTVERSION   = 1.0
EXTCOMMENT   = Monitors execution costs of plans

MODULES      = $(EXTENSION)
MODULEDIR    = $(EXTENSION)
DOCS         = README.rst
DATA_built   = $(EXTENSION)--$(EXTVERSION).sql $(EXTENSION)--unpackaged--$(EXTVERSION).sql
REGRESS      = $(EXTENSION).ext
EXTRA_CLEAN  = $(EXTENSION).control

PG_CONFIG    = pg_config

BUILD_EXTENSION = $(shell $(PG_CONFIG) --version | grep -qE "8\.|9\.0" && echo no || echo yes)

ifeq ($(BUILD_EXTENSION),no)
DATA_built  =
DATA        = $(EXTENSION).sql uninstall_$(EXTENSION).sql
REGRESS     = $(EXTENSION)
EXTRA_CLEAN =
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Build some more files for extension support:
ifeq ($(BUILD_EXTENSION),yes)
# pgxs is included after variable definition and before targets, so the
# PostgreSQL default target is used (all:)

# build the extension--unpackaged--version.sql from uninstall_extension.sql
# this assumes that the extension was installed via sql script instead of
# CREATE EXTENSION.
# This won't upgrade from a previous version to the current one.
$(EXTENSION)--unpackaged--$(EXTVERSION).sql: uninstall_$(EXTENSION).sql
	sed 's/DROP /ALTER EXTENSION $(EXTENSION) ADD /' $< > $@

# this copy the extension.sql to extension--version.sql
$(EXTENSION)--$(EXTVERSION).sql: $(EXTENSION).sql
	cp $< $@

# this build extension.control from extension.control.in
$(EXTENSION).control: $(EXTENSION).control.in
	sed 's/EXTVERSION/$(EXTVERSION)/;s/EXTENSION/$(EXTENSION)/;s/EXTCOMMENT/$(EXTCOMMENT)/' $< > $@
endif

# Here we override targets
# Recent PostgreSQL got a bugfix about that, here we just abuse the upstream fix in the mean-time
# FIX HERE before PostgreSQL got the backpatch and push the latest minor, can remove this part when done
ifeq ($(BUILD_EXTENSION),yes)

install: all installdirs installcontrol installdata installdocs installscripts
ifdef MODULES
	$(INSTALL_SHLIB) $(addsuffix $(DLSUFFIX), $(MODULES)) '$(DESTDIR)$(pkglibdir)/'
endif # MODULES

installcontrol: $(addsuffix .control, $(EXTENSION))
ifneq (,$(EXTENSION))
	$(INSTALL_DATA) $^ '$(DESTDIR)$(datadir)/extension/'
endif

installdata: $(DATA) $(DATA_built)
ifneq (,$(DATA)$(DATA_built))
	$(INSTALL_DATA) $^ '$(DESTDIR)$(datadir)/$(datamoduledir)/'
endif

installdocs: $(DOCS)
ifdef DOCS
ifdef docdir
	$(INSTALL_DATA) $^ '$(DESTDIR)$(docdir)/$(docmoduledir)/'
endif # docdir
endif # DOCS

installscripts: $(SCRIPTS) $(SCRIPTS_built)
ifdef SCRIPTS
	$(INSTALL_SCRIPT) $^ '$(DESTDIR)$(bindir)/'
endif # SCRIPTS

installdirs:
ifneq (,$(EXTENSION))
	$(MKDIR_P) '$(DESTDIR)$(datadir)/extension'
endif
ifneq (,$(DATA)$(DATA_built))
	$(MKDIR_P) '$(DESTDIR)$(datadir)/$(datamoduledir)'
endif
ifneq (,$(MODULES))
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)'
endif
ifdef DOCS
ifdef docdir
	$(MKDIR_P) '$(DESTDIR)$(docdir)/$(docmoduledir)'
endif # docdir
endif # DOCS

endif

dist:
	git archive --prefix=$(EXTENSION)-$(EXTVERSION)/ -o ../$(EXTENSION)_$(EXTVERSION).orig.tar.gz HEAD

deb:
	make clean
	make -f debian/rules debian/control
	dh clean
	make -f debian/rules orig
	debuild -us -uc -sa
