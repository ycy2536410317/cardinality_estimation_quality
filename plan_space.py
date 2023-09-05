import os
import sys
from matplotlib import pyplot as plt
import psycopg2
import time
import numpy as np


def load_sql_files(directory):
    sql_files = [file for file in os.listdir(
        directory) if file.endswith('.sql')]
    sql_queries = {}

    for sql_file in sql_files:
        with open(os.path.join(directory, sql_file), 'r') as f:
            query_name = os.path.splitext(sql_file)[0]
            sql_queries[query_name] = f.read()

    return sql_queries


def run_queries(pg_url, queries, tree_shape='default'):
    connection = psycopg2.connect(pg_url)

    if not (tree_shape == 'default' or tree_shape == 'left' or tree_shape == 'right' or tree_shape == 'zig-zag'):
        print("error, tree shape must be default, left, right or zig-zag")
        os._exit(1)

    set_tree_shape = "SET pg_hint_plan.dp_tree_shape to " + tree_shape + ";"
    with connection.cursor() as cursor:
        cursor.execute(set_tree_shape)
        connection.commit()

    query_results = {}

    for query_name, query_sql in queries.items():
        print(f"Running query: {query_name}")

        sql = 'EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) ' + query_sql
        result = None 
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()[0][0][0]
        
        elapsed_time = result['Execution Time']
        query_results[query_name] = elapsed_time

        print(f"Query {query_name} finished in {elapsed_time:.4f} seconds")

    connection.close()
    return query_results


def plot_ratio_histogram(ratios):
    bins = [0.3, 0.9, 1.1, 2, 10, 100, np.inf]
    hist, _ = np.histogram(ratios, bins=bins)

    total_elements = len(ratios)
    bin_labels = ['0.3-0.9', '0.9-1.1', '1.1-2', '2-10', '10-100', '>100']

    proportions = hist / total_elements * 100

    plt.bar(bin_labels, proportions)
    plt.xlabel('Time Ratio Intervals')
    plt.ylabel('Percentage of Elements (%)')
    plt.title('Histogram of Time Ratios')
    # save histogram
    plt.savefig('histogram.png')
    plt.show()


def compare_query_times(query_results_dir1, query_results_dir2):
    ratios = []

    for query_name, time_dir1 in query_results_dir1.items():
        time_dir2 = query_results_dir2.get(query_name, None)

        if time_dir2 is not None:
            time_ratio = time_dir1 / time_dir2
            ratios.append(time_ratio)

    return np.array(ratios)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py PG_URL DIR")
        sys.exit(1)

    pg_url = sys.argv[1]
    hint_dir = sys.argv[2]

    if not os.path.exists(hint_dir):
        print("Error: Both directories must exist")
        sys.exit(1)

    queries_dir = load_sql_files(hint_dir)
    
    # run query in different tree shape option
    query_results_dir1 = run_queries(pg_url, queries_dir, 'default')
    query_results_dir2 = run_queries(pg_url, queries_dir, 'left')
    query_results_dir3 = run_queries(pg_url, queries_dir, 'right')
    query_results_dir4 = run_queries(pg_url, queries_dir, 'zig-zag')
    
    # compare the time ratio to default one
    ratios1 = compare_query_times(query_results_dir1, query_results_dir2)
    ratios2 = compare_query_times(query_results_dir1, query_results_dir3)
    ratios3 = compare_query_times(query_results_dir1, query_results_dir4)
    
    # compute the median, 95% and max and write log to file
    print("default vs left")
    print("median: ", np.median(ratios1))
    print("95%: ", np.percentile(ratios1, 95))
    print("max: ", np.max(ratios1))
    print("default vs right")
    print("median: ", np.median(ratios2))
    print("95%: ", np.percentile(ratios2, 95))
    print("max: ", np.max(ratios2))
    print("default vs zig-zag")
    print("median: ", np.median(ratios3))
    print("95%: ", np.percentile(ratios3, 95))
    print("max: ", np.max(ratios3))
    

    
