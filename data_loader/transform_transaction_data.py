import logging
import pandas as pd
import numpy as np

class DataTransformer:
    """
    A class used to transform transaction data.

    Attributes
    ----------
    df_transactions : pd.DataFrame
        a DataFrame containing transaction data.

    Methods
    -------
    frequency_encode(group_per_time: str):
        Frequency encodes the daily transactions.
    """

    def __init__(self, df_transactions: pd.DataFrame) -> None:
        self.df_transactions = df_transactions
        logging.info("Transformer object is created")
        

    def frequency_encode(self, group_per_time: str = "D") -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Frequency encodes the daily transactions.

        Parameters
        ----------
        group_per_time : str
            The frequency for grouping transactions. Default is 'D' for daily.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            A tuple containing the transformed X and y data.
        """
        grouped_transactions = self.df_transactions.groupby([
            pd.Grouper(key="SaleDate", freq=group_per_time),
            pd.Grouper(key="Location")
        ])

        # Generate frequency encoding for y
        product_id_list = pd.DataFrame(grouped_transactions["ProductId"].apply(list))
        y = [dict(pd.Series(product_ids).value_counts()) for product_ids in product_id_list["ProductId"]]
        df_y = pd.DataFrame(y).fillna(0).set_index(product_id_list.index)

        # Generate X
        constant_unique_columns = (grouped_transactions.nunique() <= 1).all()
        constant_columns = np.unique((np.append(constant_unique_columns.loc[constant_unique_columns == True].index, ["Latitude", "Longitude", "tavg", "prcp"]))) # fix that there are locations that are widely spread apart
        df_x = grouped_transactions[constant_columns].first()
        
        logging.info("frequency encoded data has loaded")
        return df_x, df_y