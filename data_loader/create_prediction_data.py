import logging
from dataclasses import dataclass, field
from data_loader.auxiliary_data_loader import AuxDataLoader
from data_loader.transaction_data_loader import REQUIRED_TRANSACTION_COLUMNS

import pandas as pd 
from datetime import datetime, timedelta

@dataclass
class PredictionData:
    """
    A dataclass that holds the data needed to make a prediction.

    Attributes
    ----------
    days_of_prediction : int
        The number of days in the future for which to make predictions.
    df_prediction_transactions : pd.DataFrame
        A DataFrame containing the base transactions for each location.

    Methods
    -------
    create_base_transactions_for_each_location():
        Creates a DataFrame in the form of the model transactions.
    """
    days_of_prediction: int
    df_prediction_transactions: pd.DataFrame = field(init=False, default_factory=pd.DataFrame)

    def __post_init__(self):
        self.df_prediction_transactions = self.create_base_transactions_for_each_location()
        logging.info("PredictionData created")

    

    def create_base_transactions_for_each_location(self) -> pd.DataFrame:
        """
        Creates a DataFrame in the form of the model transactions.

        Returns
        -------
        df_prediction_transactions : pd.DataFrame
            A DataFrame containing the base transactions for each location.
        """
        ADL = AuxDataLoader()
        df_machines = ADL.load_machine_information().reset_index(drop=False)
        df_stock = ADL.load_location_stock().reset_index(drop=False)\
            .drop(columns=["MaxCount","AvailableCount","DateTimeStock"])
        df_base_transactions = pd.merge(df_machines, df_stock, on="Location")

        df_prediction_transactions = self._cartesian_product(df_base_transactions, self._generate_dates())
        
        df_prediction_transactions = self.order_columns_to_transactions_format(df_prediction_transactions)

        return df_prediction_transactions

    def _generate_dates(self) -> pd.DataFrame:
        """
        Generates a DataFrame containing the dates for which to make predictions.

        Returns
        -------
        df_dates : pd.DataFrame
            A DataFrame containing the dates for which to make predictions.
        """
        start = datetime.today().replace(hour=18, minute=0, second=0, microsecond=0) + timedelta(days=1)
        end = datetime.today().replace(hour=18, minute=0, second=0, microsecond=0) + timedelta(days=self.days_of_prediction + 1)

        dates = pd.date_range(start=start, end=end, freq='D')
        df_dates = pd.DataFrame(dates, columns=['SaleDate'])

        return df_dates
    
    def order_columns_to_transactions_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Orders the columns of a the transaction to the required transaction format

        REQUIRED_TRANSACTION_COLUMNS is defined in the transaction_data_loader .
        
        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to order.
        Returns
        -------
        df : pd.DataFrame
            The ordered DataFrame.
        """
        columns = REQUIRED_TRANSACTION_COLUMNS
        
        df = df[columns]

        return df

    @staticmethod
    def _cartesian_product(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Returns the cartesian product of two DataFrames.

        Parameters
        ----------
        df1 : pd.DataFrame
            The first DataFrame.
        df2 : pd.DataFrame
            The second DataFrame.

        Returns
        -------
        df : pd.DataFrame
            The cartesian product of df1 and df2.
        """
        df1['tmp'] = 1
        df2['tmp'] = 1
        df = pd.merge(df1, df2, on='tmp').drop('tmp', axis=1)

        return df