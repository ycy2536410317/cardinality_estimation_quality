#! /usr/bin/env sh

# This script installs the python dependencies
# Necessary because some dependencies can't be handled automatically by pip

pip install -r requirements.txt
pip install git+https://github.com/psycopg/psycopg2.git@master
