from matplotlib.pylab import f
import pandas as pd
from azure_connectors.AzureSqlCommunicator import (
    execute_query_and_load_results_into_dataframe,
    connect_azure,
    get_query_from_file
)

class AuxDataLoader:
    """
    A class used to load auxiliary data from Azure SQL database.
    auxiliary data is data that is not queried directly for the prediction, 
    but is needed in the process of making make the prediction.
    Think of this as price data of products, or machine information.
    ...

    Attributes
    ----------
    connection : pyodbc.Connection
        a pyodbc connection object to Azure SQL database

    Methods
    -------
    load_machine_information():
        Returns a DataFrame with all machine information.
    load_location_stock():
        Returns the stock per location in a DataFrame.
    """

    def __init__(self) -> None:
        """
        Constructs all connection to the azure databse.

        ...

        Raises
        ------
        Exception
            If connection to Azure SQL database fails.
        """
        try:
            self.connection = connect_azure()
        except Exception as e:
            raise Exception("Failed to connect to Azure SQL database.") from e

    def load_machine_information(self) -> pd.DataFrame:
        """
        Returns a DataFrame with all machine information.
        Uses the sql/machine_data.sql query to get the machine information from the database.
        is used in both enrighiching data as well as creating a prediction_data
        ...

        Returns
        -------
        df_machines : pd.DataFrame
            a DataFrame with MachineId as index and MachineName, Latitude, Longitude, Location, 
            LocationType, Environment, InServiceHours, InServiceDays as columns.

        Raises
        ------
        Exception
            If query execution fails.
        """
        try:
            query = get_query_from_file("sql/machine_data.sql")
            df_machines = execute_query_and_load_results_into_dataframe(self.connection, query)
            df_machines.set_index("MachineId", inplace=True)
            return df_machines
        except Exception as e:
            raise Exception("Failed to execute query and load machine information.") from e

    def load_location_stock(self) -> pd.DataFrame:
        """
        Returns the stock per location in a DataFrame.

        ...

        Returns
        -------
        df_location_stock : pd.DataFrame
            a DataFrame with Location and ProductId as multi-index and AvailableCount, MaxCount as columns.

        Raises
        ------
        Exception
            If query execution fails.
        """
        try:
            query = get_query_from_file("sql/stock_per_location.sql")
            df_location_stock = execute_query_and_load_results_into_dataframe(self.connection, query)
            df_location_stock.set_index(['Location', 'ProductId'], inplace=True)
            return df_location_stock
        except Exception as e:
            raise Exception("Failed to execute query and load location stock information.") from e
        
    def load_gross_product_profit_lookup(self) -> dict:
        """
        Returns the profit per product in a dictionary.

        ...

        Returns
        -------
        df_product_profit : pd.DataFrame
            a DataFrame with ProductId as index and Profit as column.

        Raises
        ------
        Exception
            If query execution fails.
        """
        try:
            query = get_query_from_file("sql/stock_per_location.sql")
            df_location_stock = execute_query_and_load_results_into_dataframe(self.connection, query)
            df_location_stock.set_index("ProductId", inplace=True)
            gross_profit_lookup_dict = dict(df_location_stock.to_dict()["GrossProfit"])
            gross_profit_lookup_dict = AuxDataLoader.test_gross_profit_lookup_dict(gross_profit_lookup_dict)
            return gross_profit_lookup_dict
        except Exception as e:
            raise Exception("Failed to execute query and load product profit information.") from e
        
    @staticmethod
    def test_gross_profit_lookup_dict(gross_profit_lookup_dict):
        for key, value in gross_profit_lookup_dict.items():
            if not isinstance(value, float):
                warnings.warn(f"Product {key} has {value} which is not of type float. Setting it to 0.")
                gross_profit_lookup_dict[key] = 0.0
            elif value < 0:
                warnings.warn(f"Product {key} has {value} GrossProfit which is negative. Setting it to 0.")
                gross_profit_lookup_dict[key] = 0.0
        return gross_profit_lookup_dict