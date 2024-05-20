import sys
import json
import csv
import yaml

import copy

import os

import pandas as pd
import numpy as np
import math

import itertools

import matplotlib as mpl

import time
from datetime import datetime
# see https://stackoverflow.com/questions/415511/how-do-i-get-the-current-time-in-python
#   for some basics about datetime

import pprint

# sqlalchemy 2.0 documentation: https://www.sqlalchemy.org/
import psycopg2
from sqlalchemy import create_engine, text as sql_text


# ==============================

def build_query_add_index(column, table):
    query = f"""
BEGIN TRANSACTION;
CREATE INDEX IF NOT EXISTS {column}_in_{table}
ON {table}({column});
END TRANSACTION;
"""
    return query


def build_query_drop_index(column, table):
    query = f"""
BEGIN TRANSACTION;
DROP INDEX IF EXISTS {column}_in_{table};
END TRANSACTION;
"""
    return query


def build_query_reviews_datetime_update(interval, operator, neighbourhood_type, neighborhood_val):
    if neighbourhood_type == 'group':
        nbhood_or_group = 'neighbourhood_group'
    else:
        nbhood_or_group = 'neighbourhood'

    query = f"""
UPDATE reviews r
SET datetime = datetime {operator} INTERVAL '{interval} days'
FROM listings l
WHERE l.id = r.listing_id
AND l.{nbhood_or_group} = '{neighborhood_val}'
RETURNING 'done';
"""
    return query


# Add or drop index
def add_drop_index(db_eng, add_drop, column, table):
    with db_eng.connect() as conn:
        if add_drop == 'add':
            conn.execute(sql_text(build_query_add_index(column, table)))
        elif add_drop == 'drop':
            conn.execute(sql_text(build_query_drop_index(column, table)))
    return


# build index description key
def build_index_description_key(all_indexes, spec):
    key = '__'
    for index in sorted(all_indexes, key=lambda x: x[1]):
        if (index in spec) or (index == spec):
            if index[0] in ['neighbourhood', 'neighbourhood_group']:
                col = 'neigh'
            else:
                col = index[0]
            table = index[1]
            key += f'{col}_in_{table}__'
    return key


# Calculate the difference between times
def time_diff(time1, time2):
    return (time2-time1).total_seconds()


def calc_perf_per_update_datetime_query(db_eng, count, query):
    exec_time = datetime.now()
    time_list = []

    add_query = query[0]
    sub_query = query[1]

    for _ in range(math.ceil(count/2)):
        # add query
        time_start = datetime.now()

        with db_eng.connect() as conn:
            conn.execute(sql_text(add_query))

        time_end = datetime.now()

        # calculate the time difference
        diff = time_diff(time_start, time_end)
        time_list.append(diff)

        # sub query
        time_start = datetime.now()

        with db_eng.connect() as conn:
            conn.execute(sql_text(sub_query))

        time_end = datetime.now()

        # calculate the time difference
        diff = time_diff(time_start, time_end)
        time_list.append(diff)

    # calculate the metrics
    perf_profile = {
        'avg': round(sum(time_list) / len(time_list), 4),
        'min': round(min(time_list), 4),
        'max': round(max(time_list), 4),
        'std': round(np.std(time_list), 4),
        'count': count,
        'timestamp': exec_time.strftime("%Y-%m-%d-%H:%M:%S")
    }
    return perf_profile


def run_update_datetime_query_neighbourhoods(db_eng, count, q_dict, all_indexes, filename):
    # get all combinations of indexes
    specs = []
    for i in range(0, len(all_indexes)+1):
        specs += (list(itertools.combinations(all_indexes, i)))

    # run the tests
    # fetch the current performance data, if any
    if os.path.exists('perf_data/' + filename):
        perf_summary = fetch_perf_data(filename)
    else:
        perf_summary = {}

    # run each query with each index spec
    for query in q_dict:
        if query in perf_summary:
            perf_dict = perf_summary[query]
        else:
            perf_dict = {}

        for spec in specs:
            print(f'\nRunning test for query "{query}" with indexes {spec}')
            # remove any current indexes not in spec
            for index in all_indexes:
                if (index not in spec) or (index != spec):
                    add_drop_index(db_eng, 'drop', index[0], index[1])

            # add any new indexes in spec
            if spec:
                for index in spec:
                    add_drop_index(db_eng, 'add', index[0], index[1])

            # build the key for the index description
            key = build_index_description_key(all_indexes, spec)

            # run the test
            perf_dict[key] = calc_perf_per_update_datetime_query(
                db_eng, count, q_dict[query])

        # add the results to perf_summary
        perf_summary[query] = perf_dict

    # write the results to the file
    write_perf_data(perf_summary, filename)

    return


# Fetches the json file
def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)


# writes the dictionary in dict as a json file into filename
def write_perf_data(dict1, filename):
    dict2 = dict(sorted(dict1.items()))
    with open('perf_data/' + filename, 'w') as fp:
        json.dump(dict2, fp, indent=4)
