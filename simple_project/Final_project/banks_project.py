# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import numpy as np 
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
export_cvs = './Largest_banks_data.csv'
load_cvs = './exchange_rate.csv'
log_file = 'code_log.txt'
columns = ["Name","MC_USD_Billion"]
query_statement1 = f"SELECT * FROM {table_name}"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement3 = f"SELECT Name from {table_name} LIMIT 5"


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    datetime_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp              
    timestamp = now.strftime(datetime_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')   

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            a_tag = col[1].find_all('a')
            if a_tag:
                name = a_tag[-1].get_text(strip=True)
                mc = float(col[2].contents[0])
                data_dict = {"Name": name, 
                             "MC_USD_Billion": mc}
                
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    print(df)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    cvs_df = pd.read_csv(csv_path)
    exchange_rate = cvs_df.set_index('Currency').to_dict()['Rate']
    print(exchange_rate)

    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    
    print(df)
    print(f"df['MC_EUR_Billion'][4] = {df['MC_EUR_Billion'][4]}")

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path) 

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False) 

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    result = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(result)   
    log_progress("Process Complete")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")
extracted_data = extract(url, columns)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data = transform(extracted_data, load_cvs)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, export_cvs)
log_progress("Data saved to CSV file")

log_progress("SQL Connection initiated")
conn = sqlite3.connect(db_name)
load_to_db(transformed_data, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

run_query(query_statement1,conn)
run_query(query_statement2,conn)
run_query(query_statement3,conn)

conn.close()    
log_progress("Server Connection closed")