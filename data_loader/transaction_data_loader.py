import os
import pandas as pd
from data_loader.Type_guard import string_guard, integer_guard, float_guard, object_guard, datetime_guard
from azure_connectors.AzureSqlCommunicator import execute_query_and_load_results_into_dataframe, \
    connect_azure, get_query_from_file
import logging

REQUIRED_TRANSACTION_COLUMNS = ['ProductId', 'ProductName', 'PackagingType', 
                'Brand', 'ProductCategory','GrossProfit', 'SaleDate', 
                'MachineId', 'MachineName', 'Latitude', 'Longitude', 'Location',
                'LocationType', 'Environment','InServiceHours', 'InServiceDays'
                ]

class ModelDataLoader:
    """
    A class used to load data for the model.

    Attributes
    ----------
    connection : object
        The connection object to the Azure SQL database.

    Methods
    -------
    load_transactions():
        Loads transaction data from the Azure SQL database.
    _test_transactions(df_training_data: pd.DataFrame):
        Tests the loaded transaction data to ensure it has the correct format and data types.
    load_model_data():
        Loads the model data, which currently is just the transaction data.
    """

    def __init__(self) -> None:
        self.connection = connect_azure()


    def load_transactions(self) -> pd.DataFrame:
        """
        Loads transaction data from the Azure SQL database.

        Returns
        -------
        pd.DataFrame
            The loaded transaction data.
        """
        query = get_query_from_file("sql/load_training_data.sql")
        df_transactions = execute_query_and_load_results_into_dataframe(self.connection, query)

        self._test_transactions(df_transactions)
        logging.info("Transactions are loaded")
        return df_transactions

    def _test_transactions(self, df_training_data: pd.DataFrame) -> None:
        """
        Tests the loaded transaction data to ensure it has the correct format and data types.

        Parameters
        ----------
        df_training_data : pd.DataFrame
            The loaded transaction data.
        """
        required_columns = REQUIRED_TRANSACTION_COLUMNS
        for column in required_columns:
            assert column in df_training_data.columns, f"{column} is not in the DataFrame"
            
        for i, column in enumerate(df_training_data.columns):
            assert column == required_columns[i], f"{column} is not at required column position {i}"

        df_training_data = df_training_data.convert_dtypes(convert_string=False) 

        integer_guard(df_training_data['ProductId']) 
        object_guard(df_training_data['ProductName'])
        datetime_guard(df_training_data['SaleDate'])
        integer_guard(df_training_data['MachineId'])
        object_guard(df_training_data['MachineName'])
        float_guard(df_training_data['Latitude'])
        float_guard(df_training_data['Longitude'])
        object_guard(df_training_data['Location'])
        object_guard(df_training_data['LocationType'])
        object_guard(df_training_data['Environment'])
        object_guard(df_training_data['InServiceHours'])
        object_guard(df_training_data['InServiceDays'])

    def load_model_data(self) -> pd.DataFrame:
        """
        Loads the model data, which currently the transaction data, which is maximaly enriched with the possibilieties in the database.

        Returns
        -------
        pd.DataFrame
            The loaded model data.
        """
        logging.info("Model data is loaded")
        return self.load_transactions()