EXTENSION = multimaster
DATA = multimaster--1.0.sql
OBJS = src/multimaster.o src/dmq.o src/commit.o src/bytebuf.o src/bgwpool.o \
src/pglogical_output.o src/pglogical_proto.o src/pglogical_receiver.o \
src/pglogical_apply.o src/pglogical_hooks.o src/pglogical_config.o \
src/pglogical_relid_map.o src/ddd.o src/bkb.o src/spill.o src/state.o \
src/resolver.o src/ddl.o src/syncpoint.o src/global_tx.o src/mtm_utils.o
MODULE_big = multimaster

ifndef USE_PGXS # hmm, user didn't requested to use pgxs
# relative path to this makefile
mkfile_path := $(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST))
# relative path to dir with this makefile
mkfile_dir := $(dir $(mkfile_path))
# abs path to dir with this makefile
mkfile_abspath := $(shell cd $(mkfile_dir) && pwd -P)
# parent dir name of directory with makefile
parent_dir_name := $(shell basename $(shell dirname $(mkfile_abspath)))
ifneq ($(parent_dir_name),contrib) # a-ha, but the extension is not inside 'contrib' dir
USE_PGXS := 1 # so use it anyway, most probably that's what the user wants
endif
endif
# $(info) is introduced in 3.81, and PG doesn't support makes older than 3.80
# ifeq ($(MAKE_VERSION),3.80)
# $(warning $$USE_PGXS is [${USE_PGXS}] (we use it automatically if not in contrib dir))
# else
# $(info $$USE_PGXS is [${USE_PGXS}] (we use it automatically if not in contrib dir))
# endif

ifdef USE_PGXS # use pgxs
# You can specify path to pg_config in PG_CONFIG var
ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
PG_CPPFLAGS += -I$(CURDIR)/src/include
# add installation top include directory for libpq header
# (seems like server/ dir is added by pgxs)
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK += -lpq # add libpq
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

else # assume the extension is in contrib/ dir of pg distribution
PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/src/include
PG_CPPFLAGS += -I$(libpq_srcdir) # include libpq-fe, defined in Makefile.global.in
SHLIB_LINK = $(libpq) # defined in Makefile.global.in
subdir = contrib/mmts
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
# in ee, install pathman as well
ifeq (${PGPRO_EDITION}, enterprise)
EXTRA_INSTALL=contrib/pg_pathman
endif
include $(top_srcdir)/contrib/contrib-global.mk
endif # USE_PGXS

REGRESS_SHLIB=$(abs_top_builddir)/src/test/regress/regress$(DLSUFFIX)
export REGRESS_SHLIB

.PHONY: all

# recurse down to referee/ on install.
# (I'd use $(call recurse...), but how can we pass USE_PGXS there?
referee-install:
	USE_PGXS=$(USE_PGXS) $(MAKE) -C referee install
install: referee-install

all: multimaster.so

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all
	$(MAKE) -C $(top_builddir)/src/test/regress tablespace-setup

# all .pl tests should pass now, but let's see what the buildfarm says
# ifndef MTM_ALL
# PROVE_TESTS ?=
# endif
PROVE_FLAGS += --timer
ifndef USE_PGXS
check: temp-install submake-regress
	$(prove_check)
else # pgxs build
# Note that for PGXS build we override here bail-out recipe defined in pgxs.mk,
# but well, why should we chose another name?
# submake-regress won't work as we have no access to the source; we assume
# regress is already installed
# final spell is inspired by
# https://www.2ndquadrant.com/en/blog/using-postgresql-tap-framework-extensions/
# and Makefile.global.in which is obviously the original source
check:
	rm -rf '$(CURDIR)'/tmp_check
	$(MKDIR_P) '$(CURDIR)'/tmp_check
	PGXS=$(PGXS) TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)
endif

# PG_PROVE_FLAGS adds PostgresNode and friends include dir
start: temp-install
	rm -rf '$(CURDIR)'/tmp_check
	$(MKDIR_P) '$(CURDIR)'/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' \
		$(with_temp_install) \
		PG_REGRESS='$(CURDIR)/$(top_builddir)/src/test/regress/pg_regress' \
		perl $(PG_PROVE_FLAGS) run.pl --action=start $(RUN_OPTS)

stop:
	cd $(srcdir) && TESTDIR='$(CURDIR)' \
		PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' \
		perl $(PG_PROVE_FLAGS) run.pl --action=stop $(RUN_OPTS)

# for manual testing: runs core regress tests on 'make start'ed cluster
run-pg-regress: submake-regress
	cd $(CURDIR)/$(top_builddir)/src/test/regress && \
	$(with_temp_install) \
	PGPORT='65432' \
	PGHOST='127.0.0.1' \
	PGUSER='$(USER)' \
	./pg_regress \
	--bindir='' \
	--use-existing \
	--schedule=$(abs_top_srcdir)/src/test/regress/parallel_schedule \
	--dlpath=$(CURDIR)/$(top_builddir)/src/test/regress \
	--inputdir=$(abs_top_srcdir)/src/test/regress

# for manual testing: runs contrib/test_partition on 'make start'ed cluster
run-pathman-regress:
	cd $(CURDIR)/$(top_builddir)/src/test/regress && \
	$(with_temp_install) \
	PGPORT='65432' \
	PGHOST='127.0.0.1' \
	PGUSER='$(USER)' \
	./pg_regress \
	--bindir='' \
	--use-existing \
	--temp-config=$(abs_top_srcdir)/contrib/test_partition/pg_pathman.add \
	--inputdir=$(abs_top_srcdir)/contrib/test_partition/ \
	partition


# bgw-based partition spawning is not supported by mm, so I
# commenting out body of set_spawn_using_bgw() sql function before
# running that
run-pathman-regress-ext:
	cd $(CURDIR)/$(top_builddir)/src/test/regress && \
	$(with_temp_install) \
	PGPORT='65432' \
	PGHOST='127.0.0.1' \
	PGUSER='$(USER)' \
	./pg_regress \
	--bindir='' \
	--use-existing \
	--temp-config=$(abs_top_srcdir)/contrib/pg_pathman/conf.add \
	--inputdir=$(abs_top_srcdir)/contrib/pg_pathman/ \
	pathman_array_qual pathman_basic pathman_bgw pathman_calamity pathman_callbacks \
	pathman_column_type pathman_cte pathman_domains pathman_dropped_cols pathman_expressions \
	pathman_foreign_keys pathman_gaps pathman_inserts pathman_interval pathman_join_clause \
	pathman_lateral pathman_hashjoin pathman_mergejoin pathman_only pathman_param_upd_del \
	pathman_permissions pathman_rebuild_deletes pathman_rebuild_updates pathman_rowmarks \
	pathman_runtime_nodes pathman_subpartitions pathman_update_node pathman_update_triggers \
	pathman_upd_del pathman_utility_stmt pathman_views

pg-regress: | start run-pg-regress
pathman-regress: | start run-pathman-regress-ext stop
installcheck:
	$(prove_installcheck)
