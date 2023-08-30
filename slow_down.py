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


def run_queries(pg_url, queries):
    connection = psycopg2.connect(pg_url)
    query_results = {}

    for query_name, query_sql in queries.items():
        print(f"Running query: {query_name}")
        start_time = time.time()
        obj = None
        with connection.cursor() as cursor:
            cursor.execute(query_sql)
            obj = cursor.fetchall()
        end_time = time.time()
        elapsed_time = end_time - start_time
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
    if len(sys.argv) != 4:
        print("Usage: python script.py PG_URL DIR1 DIR2")
        sys.exit(1)

    pg_url = sys.argv[1]
    dir1 = sys.argv[2]
    dir2 = sys.argv[3]

    if not os.path.exists(dir1) or not os.path.exists(dir2):
        print("Error: Both directories must exist")
        sys.exit(1)

    queries_dir1 = load_sql_files(dir1)
    queries_dir2 = load_sql_files(dir2)

    print("Running queries from dir1:")
    query_results_dir1 = run_queries(pg_url, queries_dir1)

    print("\nRunning queries from dir2:")
    query_results_dir2 = run_queries(pg_url, queries_dir2)

    ratios = compare_query_times(query_results_dir1, query_results_dir2)
    plot_ratio_histogram(ratios)
