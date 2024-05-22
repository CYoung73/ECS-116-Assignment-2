import sys
import json
import csv
import yaml

import copy

import pandas as pd
import numpy as np

import matplotlib as mpl

import time
from datetime import datetime

import pprint

import psycopg2
from sqlalchemy import create_engine, text as sql_text


# ==============================

# === STEP 2 FUNCTIONS ===
# Simple check
def hello_world():
    print("Hello World!")
    

# Join listing and reviews table
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


# Add/drop indexes and show result
def add_drop_index(db_eng, add_drop, column, table):
    if add_drop == 'add':
        q1 = """BEGIN TRANSACTION;
        CREATE INDEX IF NOT EXISTS """
        q2 = """ ON """
        q3 = """;
        END TRANSACTION;"""
        query = q1+column+'_in_'+table+q2+table+'('+column+')'+q3
    if add_drop == 'drop':
        q1 = """BEGIN TRANSACTION;
        DROP INDEX IF EXISTS """
        q2 = """;
        END TRANSACTION;"""
        query = q1+column+'_in_'+table+q2
    qs1 = """
    select *
    from pg_indexes
    where tablename = '"""
    qs2 = """';"""
    show_index_query = qs1+table+qs2
    with db_eng.connect() as conn:
        conn.execute(sql_text(query))
        result = conn.execute(sql_text(show_index_query))
    return result.all()


# Build listing of strings corresponding to the entries in spec, and concatenates them in the ordering given by all_indexes
def build_index_description_key(all_indexes, spec):
    order = sorted(spec, key = lambda x: all_indexes.index(x))
    strings = []
    for col, table in order:
        string = col+'_in_'+table
        strings.append(string)
    return '__' + '__'.join(strings) + '__'


# Fetches the json file
def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)


# Writes the dictionary in dict as a json file into filename
def write_perf_data(dict1, filename):
    dict2 = dict(sorted(dict1.items()))
    with open('perf_data/' + filename, 'w') as fp:
        json.dump(dict2, fp)

# ==============================

# === STEP 3a FUNCTIONS ===

# ==============================

# === STEP 3b FUNCTIONS ===
# Function for building queries - either using text search vector or using 'ILIKE'
def build_text_search_query(query_type, date1, date2, word):
    if query_type == 'tsv':
        q_11 = """select count(*)
    from reviews r
    where comments_tsv @@ to_tsquery('"""
        q_12 = """')
    and datetime >= '"""
        q_13 = """'
    and datetime <= '"""
        q_14 = """';"""
        query = q_11+word+q_12+date1+q_13+date2+q_14
    if query_type == 'like':
        q_21 = """select count(*)
        from reviews r
        where comments ILIKE '%%"""
        q_22 = """%%'
        and datetime >= '"""
        q_23 = """'
        and datetime <= '"""
        q_24 = """';"""
        query = q_21+word+q_22+date1+q_23+date2+q_24
    return query


# Runs function multiple times and gets the performance metrics
def get_perf_metrics(db_eng, query,count):
    time_list = []
    for i in range(0,count):
        time_start = datetime.now()
        # Open new db connection for each execution of the query to avoid multithreading
        with db_eng.connect() as conn:
            df = pd.read_sql(query, con=conn)
        time_end = datetime.now()
        diff = time_diff(time_start, time_end)
        time_list.append(diff)
        
    perf_profile = {
        'avg': round(sum(time_list) / len(time_list), 4),
        'min': round(min(time_list), 4),
        'max': round(max(time_list), 4),
        'std': round(np.std(time_list), 4),
        'exec_count': count,
        'timestamp': datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    }
    return perf_profile


# Gets performance metrics for specified year, word, and count
# Runs four combinations (two different types of queries, and then without + with the datetime_in_reviews index)
def text_search(db_eng, date1, date2, word, count):
    results = {}
    q_like = build_text_search_query('like',date1,date2,word)
    q_tsv = build_text_search_query('tsv',date1,date2,word)
    
    # Drop index
    mod_index = add_drop_index(db_eng, 'drop', 'datetime', 'reviews')
    
    # LIKE
    perf_profile = get_perf_metrics(db_eng, q_like,count)
    results['__'] = perf_profile
    
    # TSV
    perf_profile = get_perf_metrics(db_eng, q_tsv,count)
    results['__comments_tsv_in_reviews__'] = perf_profile
    
    # Add index
    mod_index = add_drop_index(db_eng, 'add', 'datetime', 'reviews')
    
    # LIKE and Datetime
    perf_profile = get_perf_metrics(db_eng, q_like,count)
    results['__datetime_in_reviews__'] = perf_profile
    
    # TSV and Datetime
    perf_profile = get_perf_metrics(db_eng, q_tsv,count)
    results['__datetime_in_reviews__comments_tsv_in_reviews__'] = perf_profile
    
    return results

# ==============================
    
# === STEP 3c FUNCTIONS ===
