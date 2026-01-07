# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime 

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'
log_file = 'etl_project_log.txt'
columns = ["Country/Territory","GDP_USD_millions"]
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_table = requests.get(url).text
    data = BeautifulSoup(html_table, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            a_tag = col[0].find('a')
            gdp = col[2].contents[0]
            if a_tag:
                country = a_tag.get_text(strip=True)
                if gdp !='—':
                    data_dict = {"Country/Territory": country, 
                            "GDP_USD_millions": gdp}

                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)

    # print(df)

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',', '').astype(float)
    df['GDP_USD_millions'] = round((df.GDP_USD_millions/1000) ,2)
    df = df.rename({'GDP_USD_millions': 'GDP_USD_billions'}, axis=1)
    print(df['GDP_USD_billions'])

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

    

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. \nInitiating ETL process.")
extracted_data = extract(url, columns)
log_progress("Data extraction complete. \nInitiating Transformation process.")
transformed_data = transform(extracted_data)
log_progress("Data transformation complete. \nInitiating loading process.")


load_to_csv(transformed_data, csv_path)
log_progress("Data loading to CSV complete.")

log_progress("SQL Connection initiated.")
conn = sqlite3.connect(db_name)
load_to_db(transformed_data, conn, table_name)
log_progress("Data loaded to Database as table. Running the query.")
run_query(query_statement,conn)
log_progress("Process Complete.")
conn.close()