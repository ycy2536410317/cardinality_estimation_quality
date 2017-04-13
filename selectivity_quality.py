#!/usr/bin/env python3

'''
Connect to a postgresql database, run the provided queries on it, and
generate several plots for visualizing the quality of selectivity estimations of
predicates.
'''

import errno
import glob
import json
import matplotlib.pyplot as plt
import os
import pandas as pd
import pickle
import psycopg2
import psycopg2.extras
import seaborn
import sys

from matplotlib.backends.backend_pdf import PdfPages


QUERY_RESULTS_FILE = os.path.join(os.path.dirname(__file__), 'output', 'query_results.pkl')
GRAPHS_FILE = os.path.join(os.path.dirname(__file__), 'output', 'output.pdf')


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
    query = None # sql
    query_plan = None # json representing the plan
    planning_time = None # in milliseconds
    execution_time = None # in milliseconds
    total_cost = None
    # dataframe containing the join level and estimated and actual cardinalities
    cardinalities = None

    def __init__(self, filename):
        self.filename = filename
        with open(filename) as f:
            self.query = f.read()


    def explain(self, db):
        '''
        EXPLAIN the query in the given database to populate the execution stats fields
        '''
        result = db.explain(self.query)[0][0][0]
        self.query_plan = result['Plan']
        self.planning_time = result['Planning Time']
        self.execution_time = result['Execution Time']
        self.total_cost = result['Plan']['Total Cost']
        self.cardinalities = pd.DataFrame(self._parse_cardinalities())


    def _parse_cardinalities(self, query_plan=None):
        '''
        Read the query plan and return the list of cardinalities
        If query_plan is None, use self.query_plan. The argument is used for recursion
        '''
        if query_plan is None:
            query_plan = self.query_plan

        cardinalities = {
            'join_level': [],
            'estimated': [],
            'actual': []
        }

        # parent nodes
        try:
            for subplan in query_plan['Plans']:
                subplan_cardinalities = {}
                subplan_cardinalities = self._parse_cardinalities(subplan)
                cardinalities['join_level'] += subplan_cardinalities['join_level']
                cardinalities['estimated'] += subplan_cardinalities['estimated']
                cardinalities['actual'] += subplan_cardinalities['actual']

            max_join_level = max(cardinalities['join_level'])

            cardinalities['join_level'].append(max_join_level + 1)
            cardinalities['estimated'].append(query_plan['Plan Rows'])
            cardinalities['actual'].append(query_plan['Actual Rows'])
        # leaf nodes
        except KeyError as e:
            cardinalities['join_level'].append(0)
            cardinalities['estimated'].append(query_plan['Plan Rows'])
            cardinalities['actual'].append(query_plan['Actual Rows'])

        return cardinalities


def usage():
    help_text = '''Usage:
    {0} CONNECTION_STRING QUERIES
    {0} QUERY_RESULTS_FILE

    CONNECTION_STRING must be a libpq-valid connection string, between
    quotes.
    See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

    QUERIES must be a list of files or directories. Files must contain one and
    only one query; directories must contain .sql files containing one and only
    one query.

    If the queries have been executed before, their result has been stored in
    the file {1}. It is possible to re-use the results instead of re-executing
    all the queries by supplying the filename as argument.

    The resulting graphs are saved in {2}.

    Example:
    {0} 'host=localhost port=5432 user=postgres dbname=postgres' q1.sql q2.sql queries/
    {0} {1}
    '''.format(sys.argv[0], QUERY_RESULTS_FILE, GRAPHS_FILE)
    return help_text


def parse_query_args(query_args):
    '''
    Get the queries in the files and directories specified in query_args
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
    for i, query in enumerate(queries):
        print('Executing query ' + query.filename + '... (' + str(i+1) + '/' + str(len(queries)) + ')')
        query.explain(db)

    # save the results to re-use them later
    pickle.dump(queries, open(QUERY_RESULTS_FILE, 'wb'))


def visualize(queries):
    '''
    Generate all interesting graphs from the set of queries
    '''
    plot_functions = [
        plot_q_error_vs_join_level,
        plot_q_error_vs_query,
        plot_execution_time_vs_total_cost,
        plot_actual_vs_estimated,
    ]

    with PdfPages(GRAPHS_FILE) as pdf:
        for plot_function in plot_functions:
            plot = plot_function(queries)
            try:
                pdf.savefig(plot.figure)
            except(AttributeError):
                pdf.savefig(plot.fig)
            plt.clf()


def q_error(estimated, actual):
    '''
    Compute the q-error for the given selectivities
    Return the negative q-error if it's an underestimation, positive for
    overestimation
    '''
    # overestimation
    if estimated > actual:
        actual = max(actual, 1) # prevent division by zero
        return estimated / actual
    # underestimation
    else:
        estimated = max(estimated, 1) # prevent division by zero
        return actual / estimated * -1


def plot_q_error_vs_join_level(queries):
    # concatenate single queries cardinalities stats
    cardinalities = pd.concat([query.cardinalities for query in queries], ignore_index=True)
    # compute the q-errors and store them in the dataframe
    cardinalities['q_error'] = cardinalities.apply(lambda row: q_error(row.estimated, row.actual), axis=1)

    plot = seaborn.boxplot('join_level', 'q_error', data=cardinalities, palette='muted', linewidth=1)
    plot.set(yscale='symlog')
    plot.set_title('Plan node q-error vs its join level')
    return plot


def plot_q_error_vs_query(queries):
    # concatenate single queries cardinalities stats
    cardinalities = pd.concat([query.cardinalities.assign(filename=query.filename) for query in queries], ignore_index=True)
    # compute the q-errors and store them in the dataframe
    cardinalities['q_error'] = cardinalities.apply(lambda row: q_error(row.estimated, row.actual), axis=1)

    return seaborn.stripplot(
        y='filename',
        x='q_error',
        data=cardinalities,
        hue='join_level',
        palette=seaborn.color_palette('Blues')
    )


def plot_execution_time_vs_total_cost(queries):
    data = {
        'execution_time': [query.execution_time for query in queries],
        'total_cost': [query.total_cost for query in queries]
    }
    data = pd.DataFrame(data)

    return seaborn.lmplot('total_cost', 'execution_time', data)


def plot_actual_vs_estimated(queries):
    # concatenate single queries cardinalities stats
    cardinalities = pd.concat([query.cardinalities.assign(filename=query.filename) for query in queries], ignore_index=True)

    return seaborn.lmplot('estimated', 'actual', data=cardinalities)



if __name__ == '__main__':
    # if args are a connection string and a list of queries
    if len(sys.argv) >= 3:
        try:
            # first argument is postgresql's connection string
            pg_url = sys.argv[1]

            # all other arguments are files containing single queries or directories
            # containing those files
            queries = parse_query_args(sys.argv[2:])

        # if we don't have the correct amount of arguments, print the help text
        except(IndexError) as e:
            print(usage())
            exit(1)

        # execute the queries and collect the execution stats
        execute_queries(pg_url, queries)
    # if args is a file containing the result of queries
    else:
        try:
            #argument must be a pickle file containing the result of queries previously executed
            queries = pickle.load(open(sys.argv[1], 'rb'))
        except(IndexError):
            print(usage())
            exit(1)

    # generate all the relevant graphs
    visualize(queries)
