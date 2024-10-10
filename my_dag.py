from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(api_key, symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=********'
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

@task
def transform_data(raw_data):
    transformed_data = []
    time_series = raw_data.get('Time Series (Daily)', {})
    
    for date, values in time_series.items():
        open_price = values['1. open']
        high_price = values['2. high']
        low_price = values['3. low']
        close_price = values['4. close']
        volume = values['5. volume']
        
        # Collect the required data for each date
        transformed_data.append([date, float(open_price), float(high_price), float(low_price), float(close_price), int(volume)])
    
    return transformed_data

@task
def load_data(data, target_table):
    # Establish connection to Snowflake
    conn_cursor = return_snowflake_conn()
    cur = conn_cursor

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE OR REPLACE TABLE {target_table} (
                trade_date DATE PRIMARY KEY,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume INT
            );
        """)

        # Insert data into the table
        for record in data:
            trade_date, open_price, high_price, low_price, close_price, volume = record
            sql = f"""
                INSERT INTO {target_table} (trade_date, open_price, high_price, low_price, close_price, volume) 
                VALUES ('{trade_date}', {open_price}, {high_price}, {low_price}, {close_price}, {volume});
            """
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()

with DAG(
    dag_id='my_dag',  # Ensure this ID is valid
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ETL', 'Alpha Vantage'],
    schedule_interval='30 2 * * *'  # Change from 'schedule' to 'schedule_interval'
) as dag:

    stock_symbol = 'AMZN'  # Example stock symbol
    # Fetch API key from Variables
    api_key = Variable.get("alpha_vantage_api_key")

    # Execute ETL pipeline tasks
    raw_data = extract(api_key, stock_symbol)
    transformed_data = transform_data(raw_data)

    # Define the target table name
    target_table = "CLASSDEMO.PUBLIC.target_table"  # Replace with your target table name

    load_task = load_data(transformed_data, target_table)

