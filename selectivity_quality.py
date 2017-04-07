#!/usr/bin/env python3

'''
Connect to a postgresql database, run the provided queries on it, and
generate several plots for visualizing the quality of selectivity estimations of
predicates.
'''

import errno
import glob
import os
import sys


class Postgres():
    _connection = None
    _cursor = None

    def __init__(self, pg_url):
        self._connection = psycopg2.connect(pg_url)


def usage():
    help_text = '''Usage:
    {0} CONNECTION_STRING QUERIES

    CONNECTION_STRING must be a psycopg2-valid connection string, between
    quotes.
    See https://www.postgresql.org/docs/current/static/app-psql.html#R2-APP-PSQL-CONNECTING

    QUERIES must be a list of files or directories. Files must contain one and
    only one query; directories must contain .sql files containing one and only
    one query.

    Example:
    {0} 'host=localhost port=5432 user=postgres dbname=postgres' q1.sql q2.sql queries/
    '''.format(sys.argv[0])
    return help_text


if __name__ == '__main__':
    try:
        # first argument is postgresql's connection string
        pg_url = sys.argv[1]

        # all other arguments are files containing single queries or directories
        # containing those files
        queries = []
        query_args = sys.argv[2:]
        for query_arg in query_args:
            # if the argument is a directory, get sql files in it
            if os.path.isdir(query_arg):
                query_args += glob.glob(os.path.join(query_arg, '*.sql'))
            # if the argument is a file, read its content and add it to the
            # queries
            elif os.path.isfile(query_arg):
                with open(query_arg) as query_file:
                    queries.append(query_file.read())
            # if the argument is neither a file nor a directory, raise an
            # exception
            else:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)

        print(queries)

    # if we don't have the correct amount of arguments, print the help text
    except(IndexError):
        print(usage())
