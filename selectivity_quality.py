#!/usr/bin/env python3

'''
Connect to a postgresql database, run the provided queries on it, and
generate several plots for visualizing the quality of selectivity estimations of
predicates.
'''

import errno
import glob
import json
import os
import psycopg2
import psycopg2.extras
import sys


class Postgres():
    _connection = None
    _cursor = None

    def __init__(self, pg_url):
        self._connection = psycopg2.connect(pg_url)

    def execute(self, query):
        '''
        Execute the query and return all the results at once
        '''
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)
        return cursor.fetchall()

    def explain(self, query):
        '''
        Execute an 'EXPLAIN ANALYZE' of the query
        '''
        if not query.lower().startswith('explain'):
            query = 'EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query

        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)
        return cursor.fetchall()


class QueryResult():
    filename = None
    query = None
    query_plan = None
    planning_time = None
    execution_time = None

    def __init__(self, filename):
        self.filename = filename
        with open(filename) as f:
            self.query = f.read()


    def explain(self, db):
        '''
        EXPLAIN the query in the given database  to populate the execution stats fields
        '''
        result = db.explain(self.query)[0][0][0]
        self.query_plan = result['Plan']
        self.planning_time = result['Planning Time']
        self.execution_time = result['Execution Time']


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


def parse_query_args(query_args):
    '''
    Get the queries in the files and directories specified in quer_args
    '''
    queries = []

    for query_arg in query_args:
        # if the argument is a directory, get sql files in it
        if os.path.isdir(query_arg):
            query_args += glob.glob(os.path.join(query_arg, '*.sql'))
        # if the argument is a file, read its content and add it to the
        # queries
        elif os.path.isfile(query_arg):
            queries.append(QueryResult(query_arg))
        # if the argument is neither a file nor a directory, raise an
        # exception
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), query_arg)

    return queries


def execute_queries(pg_url, queries):
    '''
    Execute an EXPLAIN ANALYZE of each query and parse the output to get the
    relevant execution information
    '''
    db = Postgres(pg_url)
    for query in queries:
        query.explain(db)


if __name__ == '__main__':
    try:
        # first argument is postgresql's connection string
        pg_url = sys.argv[1]

        # all other arguments are files containing single queries or directories
        # containing those files
        queries = parse_query_args(sys.argv[2:])

        # execute the queries and collect the execution stats
        execute_queries(pg_url, queries)

    # if we don't have the correct amount of arguments, print the help text
    except(IndexError):
        print(usage())
