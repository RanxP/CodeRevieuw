import numpy as np
import pandas as pd

from data_loader.auxiliary_data_loader import AuxDataLoader

import logging

class ProcessPrediction :
    
    def __init__(self, prediction):
        self.prediction = prediction
        self.df_prediction = None
        logging.info("Prediction processer object has been created")
        
    def create_dataframe(self,df_with_indexes,df_with_columns)->pd.DataFrame:
        df_prediction = pd.DataFrame(self.prediction, columns=df_with_columns.columns, index=df_with_indexes.index)
        self.df_prediction = df_prediction
        logging.info("Prediction dataframe in hrf has been created")
        return df_prediction
    
    def replace_negative_predictions(self):
        self.prediction = np.where(self.prediction <= 0, 0, self.prediction)
        
    def prediction_to_cumulative_sales(self):
        if self.df_prediction is None:
            raise ValueError("The prediction dataframe has not been post innitialized")
    
        cumsum_sales = self.df_prediction.unstack(level=1).cumsum(axis=0).T
        # rename and rorder columns and indexes
        cumsum_sales = cumsum_sales.rename_axis(index={"Location":"Location",None:"ProductId"})
        cumsum_sales = cumsum_sales.reorder_levels(["Location","ProductId"],axis=0)
        
        # round sales as they can not be half a sale.
        df_predicted_sales = cumsum_sales.round(0)
        logging.info("Prediction has been transformed to cumulative sales")
        return df_predicted_sales
        
    def process_prediction(self,df_with_indexes,df_with_columns):
        self.replace_negative_predictions()
        self.create_dataframe(df_with_indexes,df_with_columns)
        df_predicted_sales = self.prediction_to_cumulative_sales()
        return df_predicted_sales
    

        
        
        
    
