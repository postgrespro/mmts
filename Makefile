EXTENSION = multimaster
DATA = multimaster--1.0.sql
OBJS = src/multimaster.o src/dmq.o src/commit.o src/bytebuf.o src/bgwpool.o \
src/pglogical_output.o src/pglogical_proto.o src/pglogical_receiver.o \
src/pglogical_apply.o src/pglogical_hooks.o src/pglogical_config.o \
src/pglogical_relid_map.o src/ddd.o src/bkb.o src/spill.o src/state.o \
src/resolver.o src/ddl.o
MODULE_big = multimaster

ifdef USE_PGXS
PG_CPPFLAGS += -I$(CURDIR)/src/include
else
PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/src/include
endif

PG_CPPFLAGS += -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/mmts
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: all

EXTRA_INSTALL=contrib/mmts

all: multimaster.so

check: temp-install
	$(prove_check)

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

run: temp-install
	rm -rf '$(CURDIR)'/tmp_check
	$(MKDIR_P) '$(CURDIR)'/tmp_check
	cd $(srcdir) && TESTDIR='$(CURDIR)' \
		$(with_temp_install) \
		MMPORT='6$(DEF_PGPORT)' \
		PG_REGRESS='$(CURDIR)/$(top_builddir)/src/test/regress/pg_regress' \
		perl run.pl --start

stop:
	cd $(srcdir) && TESTDIR='$(CURDIR)' \
		$(with_temp_install) \
		PGPORT='6$(DEF_PGPORT)' \
		PG_REGRESS='$(CURDIR)/$(top_builddir)/src/test/regress/pg_regress' \
		perl run.pl --stop

regress: submake-regress
	cd $(CURDIR)/$(top_builddir)/src/test/regress && \
	$(with_temp_install) \
	PGPORT='6$(DEF_PGPORT)' \
	PGHOST='127.0.0.1' \
	PGUSER='$(USER)' \
	./pg_regress \
	--bindir='' \
	--use-existing \
	--schedule=serial_schedule \
	--dbname=postgres