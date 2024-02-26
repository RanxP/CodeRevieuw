import config
import os
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

def create_azure_connection_url(driver, server, database, username, password):
    connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    return connection_url



def replace_sql_table_by_dataframe(connection_url, table_name, dataframe, schema = 'API'):
    engine = create_engine(connection_url, fast_executemany = True)
    try:
        dataframe.to_sql(table_name, engine, schema= schema, if_exists= 'replace', index= False)
        # config.QUERY_LOGGER.info(f'Replaced {table_name}')
    except Exception as e:
        print(e)
        # config.QUERY_LOGGER.exception(f'Error in replacing {table_name} :')
    finally:
        engine.dispose()

def append_dataframe_to_sql_table(connection_url, table_name, dataframe, schema = 'API'):
    engine = create_engine(connection_url, fast_executemany = True)
    try:
        dataframe.to_sql(table_name, engine, schema= schema, if_exists= 'append', index= False)
        # config.QUERY_LOGGER.info(f'Appended {table_name}')
    except Exception as e:
        print(e)
        # config.QUERY_LOGGER.exception(f'Error in appending {table_name} :')
    finally:
        engine.dispose()


def get_query_from_file(relative_file_path: str) -> str:
    '''Function that will read and return a query that is written/saved in a .sql file'''
    with open(relative_file_path) as file:
        query = file.read()

    return query


def execute_query_and_load_results_into_dataframe(connection_url, query):
    engine = create_engine(connection_url, fast_executemany = True)
    return pd.read_sql(sql= query, con= engine)



def run_query_file_that_replaces_existing_MySQL_table(connection_url: str, relative_file_path: str, table_name: str, schema: str) -> None:
    '''Function that will 
    1) read and return a query in .sql file. 
    2) Load the query resultsinto dataframe
    3) Insert/replace table into MySQL'''

    assert table_name in relative_file_path, 'Table name should be in the filename (relative_file_path). This is a check to prevent mismatch in table_name/query'

    query = get_query_from_file(relative_file_path)
    try:
        dataframe = execute_query_and_load_results_into_dataframe(connection_url, query)
        # config.QUERY_LOGGER.info(f'Executed query {relative_file_path}')
    except Exception as e:
        # config.QUERY_LOGGER.exception(f'Error while executing query {relative_file_path}')
        replace_sql_table_by_dataframe(connection_url=connection_url, table_name=table_name, dataframe=dataframe, schema= 'dbo')


def execute_query(connection_url, query: str):
    engine = create_engine(connection_url, fast_executemany = True)
    try:
        with Session(engine) as session, session.begin():
            result = session.execute(query)
        # config.QUERY_LOGGER.info(f'Succesfully ran query: {query}')
        return result
    except:
        print("error")
        # config.QUERY_LOGGER.exception(f'Error in executing query: {query} ')
    finally:
        engine.dispose()

    
def connect_azure():
    """Get access key for sql database"""
    pass
    return connection
