from logging import config
import pandas as pd
import warnings
import logging
from datetime import datetime

from zmq import has
from azure_connectors.AzureSqlCommunicator import replace_sql_table_by_dataframe
from azure_connectors.config import Config
from data_loader.auxiliary_data_loader import AuxDataLoader
from prediction_handeler.correct_perdiction import replace_statistical_outliers


import logging

class BusinessTranslator:

    def __init__(self, df_sales):
        self.df_sales = df_sales
        self.aux_data_loader = AuxDataLoader() 
        # self.dev_connect_str = self.connect_to_dev_db()

    def connect_to_dev_db(self):
        """Connect to the development database."""
        _config = Config()
        _config.get_database_connection_string(_config.KEYVAULT)
        dev_connect_str = _config.CONNECTION_STRING
        
        return dev_connect_str
    
    def process_sales_to_business_impact(self):
        """
        Process the sales data to generate business impact.

        Returns:
            Tuple: A tuple containing two DataFrames - df_refill_advice and df_refill_advice_per_location.
        """
        
        # replace statistical outliers 
        df_corrected_sales = replace_statistical_outliers(self.df_sales,1)
        
        # translate predicted sales to missed turnover
        df_missed_sales = self.generate_lost_sales(df_corrected_sales)
        df_missed_turnover = self.sales_to_turnover(df_missed_sales)
        df_missed_turnover_per_location = self.group_by_location(df_missed_turnover)

        # add refill advice based on possible missed turnover
        df_refill_advice = self.create_refill_advice(df_missed_turnover)
        df_refill_advice_per_location = self.create_refill_advice(df_missed_turnover_per_location)
        
        df_refill_advice = self.make_datetime_columns_names_relative_to_current_date(df_refill_advice)
        df_refill_advice_per_location = self.make_datetime_columns_names_relative_to_current_date(df_refill_advice_per_location)
        
        # add human readable product name and location name 
        df_refill_advice = self.add_product_name_to_refill_advice(df_refill_advice)
        df_refill_advice = self.add_location_name_to_refill_advice(df_refill_advice)
        
        df_refill_advice_per_location = self.add_location_name_to_refill_advice(df_refill_advice_per_location, ["Location"])
        
        # index[location, (productid)] are needed information and are dropped if not saved in columns 
        df_refill_advice.reset_index(inplace=True, drop=False)
        df_refill_advice_per_location.reset_index(inplace=True, drop=False)
        replace_sql_table_by_dataframe(self.aux_data_loader.connection, "VoorspellingLocatieProduct", df_refill_advice, schema='datascience')
        replace_sql_table_by_dataframe(self.aux_data_loader.connection, "VoorspellingLocatieOmzet", df_refill_advice_per_location, schema='datascience')
        
        return df_refill_advice, df_refill_advice_per_location
        
    
    def generate_lost_sales(self, df_predicted_sales:pd.DataFrame)->pd.DataFrame:
        """
        Generate a dataframe with the lost sales.

        The lost sales are calculated by subtracting the current stock - cumulative predicted sales.
        Unstocked products will show as nan as they cannot be subtracted due to missing index.
        The negative values are the missed sales in a given day. The positive values are the stocked products.
        The values above 0 are set to 0 as they are not lost sales, and then the values are multiplied by -1 to get the lost sales.
        """
        
        logging.info("calculating based on prediction")
        
        def create_stock_matching_predicted_sales(df_predicted_sales):
            """Create a stock dataframe with the same shape as the predicted sales
            
            The stock as returned by the sql dataframe is one row, however
            due to multiindex subtracting requires the same columns and indexes.
            hence there is a need to reformat the current stock dataframe.
            """
            current_stock = self.aux_data_loader.load_location_stock()[["AvailableCount"]]
            
            for col in df_predicted_sales.columns:
                current_stock[col] = current_stock['AvailableCount']  # Assuming 'AvailableCount' is the column to duplicate
            
            # due to the exact requirements drop the original count
            current_stock.drop('AvailableCount', axis=1, inplace=True)
                  
            return current_stock
        
        current_stock = create_stock_matching_predicted_sales(df_predicted_sales)
        
        df_lost_sales = current_stock - df_predicted_sales
        # remove unstocked products
        df_lost_sales = df_lost_sales.dropna(how='all')
        # remove predictions with products with sufficient stock
        df_lost_sales = df_lost_sales.where(df_lost_sales <= 0, 0)
        # inverse for legibility
        df_lost_sales = df_lost_sales.abs()
        return df_lost_sales 
    
    def sales_to_turnover(self, df_sales:pd.DataFrame)->pd.DataFrame:
        """
        Convert sales data to turnover data.

        Args:
            df_sales (DataFrame): DataFrame containing the sales data.

        Returns:
            DataFrame: DataFrame containing the turnover data.
        """
        
        def calculate_turnover_per_sale(row, price_lookup : dict):
            """Calculate the turnover of a product based on the number of sales and the price of a product.
            
            The price lookup is a dict with the productid as key and the price as value,
            and the sales here are cumulative.
            """
            ProductId = row.name[1]
            try:
                return row * price_lookup[int(ProductId)]
            except:
                warnings.warn(f"Product {ProductId} seems to no longer be in active inventory")
                return row * 0
            
        gross_profit_lookup_dict = AuxDataLoader().load_gross_product_profit_lookup()
        df_turnover = df_sales.apply((lambda x : 
            calculate_turnover_per_sale(x, gross_profit_lookup_dict) ),
            axis=1) 
            
        return df_turnover
    
    
    def create_refill_advice(self, df_missed_turnover:pd.DataFrame, allowable_missed_profit:float = 5) -> pd.DataFrame:
        """
        Create refill advice based on the missed turnover data.

        Args:
            df_missed_turnover (DataFrame): DataFrame containing the missed turnover data.
            allowable_missed_profit (float): The threshold for allowable missed profit.

        Returns:
            DataFrame: DataFrame with an additional column 'refill_date' indicating the refill date.
        """
        possible_refill_dates = list(df_missed_turnover.columns)
        # add a column to the dataframe to store the refill date
        df_missed_turnover["refill_date"] = None
        # reverse possible refill dates so that the closest date is last in a loop
        possible_refill_dates.reverse()
        for date in possible_refill_dates:
            # reverse turnover so the algorithm works
            mask = (df_missed_turnover[date] > allowable_missed_profit).values
            df_missed_turnover.loc[mask, "refill_date"] = date

        return df_missed_turnover
    
    @staticmethod
    def group_by_location(df):
        """
        Group the DataFrame by location.

        Args:
            df (DataFrame): DataFrame to be grouped.

        Returns:
            DataFrame: Grouped DataFrame.
        """
        return df.groupby(level=0).sum()
    
    @staticmethod
    def make_datetime_columns_names_relative_to_current_date(df:pd.DataFrame)->pd.DataFrame:
        """
        Make datetime column names relative to the current date.

        Args:
            df (DataFrame): DataFrame with datetime columns.

        Returns:
            DataFrame: DataFrame with updated column names.
        """
        col_mapper = {}
        today = datetime.today().date()
        
        for col in df.columns:
            if isinstance(col, pd.Timestamp):
                days_diff = (col.date() - today).days
                col_mapper[col] = f"Missed gross profit in +{days_diff} days"
                
        df = df.rename(columns=col_mapper)
        return df
    
    def load_product_infromation(self)->pd.DataFrame:
        """
        Load product information from the database.

        Returns:
            DataFrame: DataFrame containing the product information.
        """
        if hasattr(self, "product_information"):
            return self.product_information
        
        if not hasattr(self, "location_stock"):
            self.location_stock = self.aux_data_loader.load_location_stock()
    
        product_information = self.location_stock.reset_index()[["ProductId","ProductName"]]
        product_information = product_information.drop_duplicates(ignore_index=True)
        product_information.set_index("ProductId", inplace=True)
        product_name_lookup_dict = dict(product_information.to_dict()["ProductName"])
        return product_name_lookup_dict
    
    def load_location_information(self)->pd.DataFrame:
        """
        Load locatoin information from the database.

        Returns:
            DataFrame: DataFrame containing the product information.
        """
        if hasattr(self, "location_information"):
            return self.location_information
        
        if not hasattr(self, "location_stock"):
            self.location_stock = self.aux_data_loader.load_location_stock()
    
        location_information = self.location_stock.reset_index()[["Location","LocationName"]]
        location_information = location_information.drop_duplicates(ignore_index=True)
        location_information.set_index("Location", inplace=True)
        location_name_lookup_dict = dict(location_information.to_dict()["LocationName"])
        return location_name_lookup_dict
        
    
    def add_product_name_to_refill_advice(self, df_refill_advice:pd.DataFrame)->pd.DataFrame:
        """
        Add product name to the refill advice DataFrame.

        Args:
            df_refill_advice (DataFrame): DataFrame containing the refill advice.

        Returns:
            DataFrame: DataFrame with product name added.
        """
        df_product_refill_advice = df_refill_advice.copy()
        df_product_refill_advice = df_product_refill_advice.reset_index(drop=False)
        product_name_lookup_dict = self.load_product_infromation()
        df_product_refill_advice["ProductName"] = df_product_refill_advice["ProductId"].map(product_name_lookup_dict)
        df_product_refill_advice.set_index(["Location","ProductId"], inplace=True)

        return df_product_refill_advice
    
    def add_location_name_to_refill_advice(self, df_refill_advice:pd.DataFrame, index_names:list = ["Location","ProductId"])->pd.DataFrame:
        """
        Add location name to the refill advice DataFrame.

        Args:
            df_refill_advice (DataFrame): DataFrame containing the refill advice.

        Returns:
            DataFrame: DataFrame with location name added.
        """
        df_location_refill_advice = df_refill_advice.copy()
        df_location_refill_advice.reset_index(inplace=True)
        location_name_lookup_dict = self.load_location_information()
        df_location_refill_advice["LocationName"] = df_location_refill_advice["Location"].map(location_name_lookup_dict)
        df_location_refill_advice.set_index(index_names, inplace=True)
        return df_location_refill_advice
    
    
        