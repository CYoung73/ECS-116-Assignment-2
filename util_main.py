import sys
import json
import csv
import yaml

import datetime;
 
import copy

import pandas as pd
import numpy as np

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


# Simple check
def hello_world():
    print("Hello World!")

# Query builder
def build_query_listings_join_reviews(date1, date2):
    q31 = """SELECT DISTINCT l.id, l.name
FROM listings l, reviews r 
WHERE l.id = r.listing_id
  AND r.date >= '"""
    q32 = """'
  AND r.date <= '"""
    q33 = """'
ORDER BY l.id;"""
    return q31 + date1 + q32 + date2 + q33    

# Calculate the difference between times 
def time_diff(time1, time2):
    return (time2-time1).total_seconds()

# Calculate and return a dict with metrics for each year query 
def calc_time_diff_per_year(db_eng, count, q_dict):
    perf_details = {}

    # Iterate through all the queries in q_dict
    for query_name, sql_query in q_dict.items():
        time_list = []
        for i in range(count): 
            time_start = datetime.now()

            with db_eng.connect() as conn:
                df = pd.read_sql(sql_query, con=conn)

            time_end = datetime.now()
            # Calculate the time difference
            diff = time_diff(time_start, time_end)
            time_list.append(diff)
            
        # Calculate the metrics
        perf_profile = {
            'avg': round(sum(time_list) / len(time_list), 4),
            'min': round(min(time_list), 4),
            'max': round(max(time_list), 4),
            'std': round(np.std(time_list), 4),
            'exc_count': count,
            'timestamp': datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        }

        print(f'\nThe list of running times for {query_name} is as follows:')
        pprint.pprint(time_list)
        
        print(f'\nThe statistics on the list of running times for {query_name} are as follows:')
        pprint.pprint(perf_profile)
        
        # Add metrics according to the query name
        perf_details[query_name] = perf_profile
    
    return perf_details


def fetch_perf_data(filename):
    with open('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename, 'r') as f:
        data = json.load(f)
        sorted_data = dict(sorted(data.items()))
    return sorted_data

# writes the dictionary in dict as a json file into filename
def write_perf_data(dict1, filename):
    with open('/Users/Nfaith21/Documents/ECS 116 - Misc/' + filename, 'w') as fp:
        json.dump(dict1, fp)

'''
# Fetches the json file
def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)

# writes the dictionary in dict as a json file into filename
def write_perf_data(dict1, filename):
    dict2 = dict(sorted(dict1.items()))
    with open('/Users/Nfaith21/Downloads/DISC_5_FILES/perf_data/' + filename, 'w') as fp:
        json.dump(dict2, fp)'''

def add_drop_index(db_eng, action, column, table):

    q_show_indexes_for_reviews = f'''
    SELECT *
    FROM pg_indexes
    WHERE tablename = '{table}';
    '''
    
    if action == 'add':
        query = f'''
        BEGIN TRANSACTION;
        CREATE INDEX IF NOT EXISTS {column}_in_{table}
        ON {table}({column});
        END TRANSACTION;
        '''
    else:
        query = f'''
        BEGIN TRANSACTION;
        DROP INDEX IF EXISTS {column}_in_{table};
        END TRANSACTION;
        '''
    
    with db_eng.connect() as conn:
        conn.execute(sql_text(query))
        
        result = conn.execute(sql_text(q_show_indexes_for_reviews))
        return result.all()

def build_index_description_key(all_indexes, spec):
    sort = sorted(spec, key=lambda x: all_indexes.index(x))
    indexes = [f"{column}_in_{table}" for column, table in sort]
    return '__' + '__'.join(indexes) + '__'


def build_query_listings_join_reviews_datetime(date1, date2):
    q31 = """SELECT DISTINCT l.id, l.name
FROM listings l, reviews r 
WHERE l.id = r.listing_id
  AND r.datetime >= '"""
    q32 = """'
  AND r.datetime <= '"""
    q33 = """'
ORDER BY l.id;"""
    return q31 + date1 + q32 + date2 + q33   