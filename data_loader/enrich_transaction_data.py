import logging
import pandas as pd
import datetime
import time
from meteostat import Point, Daily
from data_loader.auxiliary_data_loader import AuxDataLoader

class DataEnricher:
    """
    A class used to enrich transaction data with additional time features and weather data.

    Attributes
    ----------
    df_transactions : pd.DataFrame
        a DataFrame containing transaction data. It is expected to have at least the following columns:
        'Location', 'SaleDate', and other transaction-related columns.
    df_machine_weather_data : pd.DataFrame
        a DataFrame containing weather data for each machine location. It is None until create_weather_data is called.

    Methods
    -------
    add_time_feature_column(timefeature: str, column_name: str):
        Adds a new column to df_transactions based on the specified time feature.
    create_weather_data(start: datetime.datetime, end: datetime.datetime, _weather_properties: list):
        Creates a DataFrame with weather data for each machine location.
    replace_unknown_weather_data():
        Placeholder method for replacing unknown weather data. Currently, it does nothing.
    add_weather_data(weather_properties: list):
        Adds weather data to df_transactions.
    """

    def __init__(self, df_transactions: pd.DataFrame) -> None:
        self.df_transactions = df_transactions
        self.df_machine_weather_data = None
        logging.info("Enriched object is created")
        
    def get_data(self):
        logging.info("Enriched data is loaded")
        return self.df_transactions

    def add_time_feature_column(self, timefeature: str,) -> None:
        """
        Adds a new column to df_transactions based on the specified time feature.

        Parameters
        ----------
        timefeature : str
            The time feature to be added. It should be one of the following: 'year', 'month', 'day', 'weekday', 'hour', 'minute', 'second'.
        column_name : str
            The name of the new column.
        """
        if timefeature.lower() not in ["year", "month", "day", "weekday", "hour", "minute", "second"]:
            raise ValueError("Timefeature not supported")

        self.df_transactions[timefeature.lower()] = getattr(self.df_transactions["SaleDate"].dt, timefeature)
        logging.info("Time feature is added : " + timefeature.lower())

    def create_weather_data(self, start: datetime.datetime, end: datetime.datetime, _weather_properties: list) -> None:
        """
        Creates a DataFrame with weather data for each machine location.

        Parameters
        ----------
        start : datetime.datetime
            The start date for the weather data.
        end : datetime.datetime
            The end date for the weather data.
        _weather_properties : list
            The weather properties to be included in the DataFrame.
        """
        if isinstance(self.df_machine_weather_data, pd.DataFrame):
            return

        df_machines = AuxDataLoader().load_machine_information()
        df_all_weather = pd.DataFrame()

        for row in df_machines.itertuples():
            if row.Latitude == 0 and row.Longitude == 0:
                continue

            meteo_data = Daily(Point(row.Latitude, row.Longitude), start, end).aggregate('1D').fetch()
            meteo_data["Location"] = row.Location
            meteo_data = meteo_data.reset_index(drop=False).rename({"temp": "tavg", "time": "SaleDate"}, axis=1)

            if not meteo_data.empty:
                df_all_weather = pd.concat([df_all_weather, meteo_data], axis=0) if not df_all_weather.empty else meteo_data.copy()

            time.sleep(0.05)  # reduce request load on the public server

        self.df_machine_weather_data = df_all_weather.convert_dtypes()
        logging.info("Weather data is loaded from the api")

    def replace_unknown_weather_data(self) -> None:
        """
        Placeholder method for replacing unknown weather data. Currently, it does nothing.
        """
        pass

    def add_weather_data(self, weather_properties: list) -> pd.DataFrame:
        """
        Adds weather data to df_transactions, based on the weather_properties list.
        
        weather_properties can be found in https://dev.meteostat.net/python/daily.html#data-structure
        and is querying on a daily basis.

        Parameters
        ----------
        weather_properties : list
            The weather properties to be added to df_transactions.
        """
        _indexed_weather_properties = ["Location", "SaleDate"] + weather_properties

        start = min(self.df_transactions.SaleDate)
        end = max(self.df_transactions.SaleDate)

        self.create_weather_data(start, end, weather_properties)
        df_enriched = pd.merge(
            self.df_transactions,
            self.df_machine_weather_data[_indexed_weather_properties],
            left_on=["Location", self.df_transactions.SaleDate.dt.date],
            right_on=["Location", self.df_machine_weather_data.SaleDate.dt.date],
            how="left",
            suffixes=('', '_date')
        )
        df_enriched.drop(columns=["key_1"], inplace=True)

        self.replace_unknown_weather_data()

        self.df_transactions = df_enriched
        logging.info("Weather data is added to the data")
        return df_enriched
    
    def add_missing_days(self): #WIP
        """Adds missing days to the data"""
        # add missing days
        df_enriched = self.df_transactions.set_index("SaleDate").groupby("Location").apply(lambda x: x.reindex(pd.date_range(min(x.index), max(x.index), freq="D"))).reset_index(level=0)
        df_enriched = df_enriched.drop(columns=["Location"]).reset_index()
        self.df_transactions = df_enriched
        logging.info("Missing days are added to the data")
        return df_enriched