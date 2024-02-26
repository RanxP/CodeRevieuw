import pandas as pd
from datetime import timedelta
from data_loader.auxiliary_data_loader import AuxDataLoader
import logging
class DataCleaner:
    """
    This class provides methods to clean transaction data

    Attributes
    ----------
    df_transactions : pd.DataFrame
        A DataFrame containing transaction data. It is expected to adhere to the standard format for transaction data.
    time_of_sales_column : str
        The name of the column in df_transactions that contains the time of sales. Default is 'SaleDate'.

    Methods
    -------
    remove_unstocked_products():
        Removes products that are no longer in stock from df_transactions.
    remove_products_with_no_recent_sales(per_machine: bool, recent_quantification: timedelta):
        Removes products with no recent sales from df_transactions. The definition of 'recent' is provided
        by the user as a timedelta object. If per_machine is True, the method considers each machine separately.
    remove_products_based_on_performance(percentage_of_sales: int, amount_of_time: timedelta, per_machine: bool):
        Removes products from df_transactions based on their sales performance over a specified time period.
        If per_machine is True, the method considers each machine separately.
    report_missing_values():
        Reports the number of missing values in each column of df_transactions.
    remove_rows_containing_nan():
        Removes rows from df_transactions that contain NaN values.
    """

    def __init__(self, df_transactions: pd.DataFrame, time_of_sales_column = "SaleDate") -> None:
        """
        Constructs all the necessary attributes for the DataCleaner object.

        Parameters
        ----------
        df_transactions : pd.DataFrame
            A DataFrame containing transaction data.
        time_of_sales_column : str, optional
            The name of the column in df_transactions that contains the time of sales. Default is 'SaleDate'.
        """
        self.df_transactions = df_transactions
        self.time_of_sales_column = time_of_sales_column
        logging.info("DataCleaner object created")
        
    def get_data(self):
        logging.info("DataCleaner object created")
        return self.df_transactions
        

    def remove_unstocked_products(self) -> None:
        """
        Removes products that are no longer in stock from df_transactions.

        This method uses the AuxDataLoader to load the current stock of all locations and removes any products from df_transactions 
        that are not in the current of each individual location.
        """
        df_stocked_products = AuxDataLoader().load_location_stock().index
        mask_stocked = self.df_transactions.set_index(['Location', 'ProductId']).index.isin(df_stocked_products)
        self.df_transactions = self.df_transactions[mask_stocked]

    def remove_products_with_no_recent_sales(self, per_machine: bool, recent_quantification: timedelta) -> None:
        """
        Removes products with no recent sales from df_transactions.

        This method removes products that have not been sold within the 'recent_quantification' time period. 
        If 'per_machine' is True, the method considers removing productes based on each machine separately.

        Parameters
        ----------
        per_machine : bool
            If True, considers each machine separately. If False, considers all machines together.
        recent_quantification : timedelta
            The time period to consider for recent sales.
        """
        cutoff_date = self.df_transactions[self.time_of_sales_column].max() - recent_quantification
        grouped_columns = ['Location', 'ProductId'] if per_machine else ['ProductId']

        # Get the date of the last sale for each product (and each machine if per_machine is True)
        df_last_sale = self.df_transactions.groupby(grouped_columns, group_keys=False)[self.time_of_sales_column].max()

        # Identify products with no recent sales
        index_old_products = df_last_sale[df_last_sale <= cutoff_date].index

        # Remove products with no recent sales from df_transactions
        self.df_transactions = self.df_transactions[~self.df_transactions.set_index(grouped_columns).index.isin(index_old_products)]
        
    def remove_products_based_on_performance(self, percentage_of_total_sales: int, 
                                            amount_of_time: timedelta, per_machine: bool ) -> None:
        """
        Removes products from df_transactions based on their sales performance.

        This method removes all transactions of products that have achieved less than the required 
        'percentage_of_total_sales' within the 'amount_of_time' time period. 
        If 'per_machine' is True, the method considers each machine separately.

        Parameters
        ----------
        percentage_of_total_sales : int
            The minimum percentage of sales a product must have to be kept.
        amount_of_time : timedelta
            The time period to consider for sales performance.
        per_machine : bool
            If True, considers each machine separately. If False, considers all machines together.
        """
        def percentage_of_sales_after_cutoff(group , cutoff):
            after_time = group[self.time_of_sales_column] > cutoff
            return after_time.sum() / len(group) * 100

        cutoff_date = self.df_transactions[self.time_of_sales_column].max() - amount_of_time
        grouped_columns = ['Location', 'ProductId'] if per_machine else ['ProductId']

        # Calculate the percentage of sales after the cutoff date for each product (and each machine if per_machine is True)
        df_sales_performance = self.df_transactions.groupby(grouped_columns).apply(percentage_of_sales_after_cutoff, cutoff_date)

        # Identify products with insufficient sales performance
        index_poor_performance = df_sales_performance[df_sales_performance < percentage_of_total_sales].index

        # Remove products with insufficient sales performance from df_transactions
        self.df_transactions = self.df_transactions[~self.df_transactions.set_index(grouped_columns).index.isin(index_poor_performance)]
        
    def split_locations_that_are_not_together(self) -> None:
        
        
        self.df_transactions.groupby("lo")
        
    def report_missing_values(self) -> None:
        """
        Reports the number of missing values in each column of df_transactions.
        """
        print(self.df_transactions.isna().sum())

    def remove_rows_containing_nan(self) -> None:
        """
        Removes rows from df_transactions that contain NaN values.
        """
        self.df_transactions.dropna(inplace=True)