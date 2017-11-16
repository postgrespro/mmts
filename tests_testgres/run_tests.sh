#!/usr/bin/bash

if [ -z "$VIRTUAL_ENV" ]; then
	>&2 echo WARNING: not in virtualenv
fi

python -m unittest discover --pattern=*.py
