#!/usr/bin/bash

if [ -z "$PG_CONFIG" ]; then
	>&2 echo ERROR: you must set PG_CONFIG
	exit 1
fi

if [ -z "$VIRTUAL_ENV" ]; then
	>&2 echo WARNING: not in virtualenv
fi

python -m unittest discover --pattern=*.py
