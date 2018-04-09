import requests
import sys
import pandas as pd
import numpy as np
import multiprocessing as mp
import time
import random
import csv

# Proof of concept of concurrent pulls of API
# https://stackoverflow.com/a/10415215/4686075


def get_from_api(api_param1, api_param2, port, proc, return_df):
    '''
    This simulates the latency of an API call.  Returns a dataframe of values (or empty dataframe if nothing to return)
    We're using this as the worker function
    :param api_param1: API parameter 1
    :param api_param2: API parameter 2
    :param port: API port
    :param proc: Process number
    :param return_df: data to return
    :return:
    '''

    # CONFIGURE THIS FOR YOUR API
    # url = 'http://api.sample.com:' + str(port) + '/'
    # payload = {'api_param1': api_param1, 'api_param2':api_param2}
    # resp = requests.get(url, params = payload)
    # if resp.status_code != 200:
    #     print("Error executing GET: {} {}".format(resp.status_code, resp.text))
    #     # return empty dataframe
    #     return_df[proc] = pd.DataFrame(np.random.randint(0, 100, size=(0, 4)), columns=['id','A','B','C'])
    # else:
    #     return_df[proc] = pd.read_json(resp.json())

    # USE THIS IF YOU WANT TO SIMULATE AN API
    time.sleep(random.randint(10,20)/10)
    if api_param1 * (api_param2-1) >= 1000: #exit after returning at least 1000 records
        return_df[proc] = pd.DataFrame(np.random.randint(0,100,size=(0, 4)), columns=['id','A','B','C'])
    else:
        return_df[proc] = pd.DataFrame(np.random.randint(0,100,size=(api_param1, 4)), columns=['id','A','B','C'])

def combine_dfs(list_df, df_cumulative):
    '''
    takes a lists of dataframes, and appends them to an existing dataframe
    :param list_df: list of data frames (each may be empty)
    :param df_cumulative: existing data frame (may be empty)
    :return: df_cumulative (if non-0 lenght), apppended with all non-0 length dataframes in list_df
    '''
    done = False
    for i in range(len(list_df)):
        if len(list_df[i]): # if the dataframe has records
            if i == 0: # if this is the first pass through the form
                df = list_df[i]
            else:
                df = df.append(list_df[i])
        else:
            df = pd.DataFrame()
            done = True
    if len(df_cumulative): # df_cumulative already has records
        if len(df): # there are new reords to add
            df_cumulative = df_cumulative.append(df)
        else: # there are no more new records to add
            done = True
    else: # df_cumulative is currently empty
        if len(df): # there are new records to add
            df_cumulative = df
        else: # there are no new records to add
            done = True
    return done, df_cumulative

def clean_and_sort(df):
    '''
    cleans the passed dataframe.  If column "A" has a "1" as any digit, prelaces the value with a double-quoted
    version of the existing value.  ie 41 becomes "41" and 212 becomes "212"
    then sorts ascending based on id
    :param df: incoming dataframe
    :return: cleaned and sorted dataframe
    '''
    df.A = df.A.apply(str)
    escape_rows = df.A.str.find('1') > -1
    df.D = '"' + df.A + '"'
    df.loc[escape_rows, ['A']] = df.D[escape_rows]
    return df.sort_values(by=['id'])


def multi_thread(api_param1, port, num_workers, fileout):
    '''
    downloads information from an API using multiple concurrent proceses
    combines those downloads into a single, cleaned and sorted dataframe
    and writes that dataframe to a csv
    :param api_param1: api parameter 1 -- how many records to return at once
    :param port: port across which the API call is made
    :param num_workers: number of concurrenntly running API  calls
    :param fileout: name of csv file to writ3
    :return: returns number of records
    '''
    #api_param1 = per_page
    #api_param2 = page
    api_param2 = 1
    sys.stdout.write('Running')
    df_cumulative = []
    jobs = []
    done = False
    while not done:
        manager = mp.Manager()
        return_df = mp.Manager().dict()
        for i in range(num_workers):
            api_param2 = api_param2 + 1
            sys.stdout.write('.')
            sys.stdout.flush()
            p = mp.Process(target=get_from_api,args=[api_param1, api_param2, port, i, return_df])
            p.start()
            jobs.append(p)
        for i in range(num_workers):
            jobs[i].join()
        list_df = return_df.values()
        jobs = []
        done, df_cumulative = combine_dfs(list_df, df_cumulative)
    print()
    df_cumulative = clean_and_sort(df_cumulative)
    df_cumulative.to_csv(fileout, index=False, quoting=csv.QUOTE_NONE)
    print("Done")
    return len(df_cumulative)

def test():
    df = multi_thread(10,8000,5,'output.csv')

if __name__ == '__main__':
    test()

