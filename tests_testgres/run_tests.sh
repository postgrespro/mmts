#!/bin/sh
set -e
ulimit -c unlimited

CURPATH=`pwd`
BASEDIR=$CURPATH/../../..
export PATH=$BASEDIR/tmp_install/usr/local/pgsql/bin/:$PATH
export DYLD_LIBRARY_PATH=$BASEDIR/tmp_install/usr/local/pgsql/lib/:$DYLD_LIBRARY_PATH
export DESTDIR=$BASEDIR/tmp_install

make -C $BASEDIR install
make -C $BASEDIR/contrib/mmts install

if [ -z "$VIRTUAL_ENV" ]; then
	>&2 echo WARNING: not in virtualenv
fi

# python3 -m unittest discover --pattern=*.py
python3 ddl.py
