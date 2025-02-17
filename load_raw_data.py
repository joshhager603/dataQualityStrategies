import psycopg2
from psycopg2 import sql
import sqlalchemy
import pandas as pd
from constants import *
import common_functions

try:

    # create a connection to the default database in Postgres (postgres)
    connection = psycopg2.connect(dbname="postgres", 
                            user=POSTGRES_USER, 
                            password=POSTGRES_PASS, 
                            host=POSTGRES_HOST, 
                            port=POSTGRES_PORT)
    connection.autocommit = True

    cursor = connection.cursor()

    # check if the source database exists already
    cursor.execute(f"SELECT * FROM pg_database WHERE datname = '{DB_NAME}'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
        print(f"Created database {DB_NAME}")
    else:
        print(f'{DB_NAME} already exists!')

    # close the connection to 'postgres' db and open a connection to the one we just made
    cursor.close()
    connection.close()

    connection = psycopg2.connect(dbname=DB_NAME, 
                            user=POSTGRES_USER, 
                            password=POSTGRES_PASS, 
                            host=POSTGRES_HOST, 
                            port=POSTGRES_PORT)
    connection.autocommit = True

    cursor = connection.cursor()

    # create the table for our raw data
    # using VARCHAR for all data to preserve raw data with its imperfections
    create_table_statement = f'''
    CREATE TABLE IF NOT EXISTS {RAW_DATA_TABLE_NAME} (
        employee_id VARCHAR(500),
        name VARCHAR(500),
        age VARCHAR(500),
        department VARCHAR(500),
        date_of_joining VARCHAR(500),
        years_of_experience VARCHAR(500),
        country VARCHAR(500),
        salary VARCHAR(500),
        performance_rating VARCHAR(500)
    );
    '''

    cursor.execute(create_table_statement)
    print(f"Table {RAW_DATA_TABLE_NAME} is created.")


    # load the raw data into the raw data table
    engine = common_functions.create_sqlalchemy_engine()
    
    df = pd.read_csv(RAW_DATA_FILEPATH)

    df.to_sql(RAW_DATA_TABLE_NAME, engine, if_exists='replace', index=False)
    print("Raw data has been inserted into the raw data table.")


except psycopg2.Error as e:
    print("An error occurred:", e)




