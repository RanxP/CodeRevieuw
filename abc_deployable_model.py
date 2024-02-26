from abc import ABC, abstractmethod
import logging

from data_loader.transaction_data_loader import ModelDataLoader
from data_loader.clean_transaction_data import DataCleaner
from data_loader.enrich_transaction_data import DataEnricher
from data_loader.transform_transaction_data import DataTransformer
from data_loader.create_prediction_data import PredictionData
from prediction_handeler.process_prediction import ProcessPrediction
from prediction_handeler.predicted_sales_impact_uploader import BusinessTranslator

import pandas as pd


class DeployableModel(ABC):
    
    def __init__(self, days_of_prediction:int = 3):
        self.df_model_data = self.load_model_data(ModelDataLoader())
        self.df_p_model_data = PredictionData(days_of_prediction).df_prediction_transactions
        

    def default_deployment(self):
        df_x, df_y = self.transform_df_model_data_to_df_x_df_y(self.df_model_data, True)
        df_p_x, df_p_y = self.transform_df_model_data_to_df_x_df_y(self.df_p_model_data, False)
        
        model = self.define_model()
        model = self.train_model(model,df_x,df_y)
        prediction = self.predict_on_model(model,df_p_x)
        
        df_prediction_hrf = self.process_prediction_to_human_readable_format(prediction,ProcessPrediction(prediction))
        self.process_hrf_to_business_impact(df_prediction_hrf, BusinessTranslator(df_prediction_hrf))
        
    @abstractmethod
    def deploy(self):
        self.default_deployment()
        
    @staticmethod
    def load_model_data(model_loader_object: ModelDataLoader) -> pd.DataFrame:
        df_model_data = model_loader_object.load_model_data()
        return df_model_data
    
    # needed sub steps for the model data
    @abstractmethod
    def clean_model_data(self, cleaner_object:DataCleaner)-> pd.DataFrame:
        pass
    
    @abstractmethod
    def enrich_model_data(self, enricher_object:DataEnricher) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def transform_model_data(self, transformer_object:DataTransformer, train:bool) -> tuple[pd.DataFrame, pd.DataFrame]:
        pass
    
    # standardize procedure for processing transormation dataframe to trainable data
    def transform_df_model_data_to_df_x_df_y(self,df_model_data:pd.DataFrame, train:bool) -> tuple[pd.DataFrame, pd.DataFrame]:
        if train:
            logging.info("Training data is being manipulated") 
        else:
            logging.info("Prediction data is being manipulated")
            
        df_model_data = self.clean_model_data(DataCleaner(df_model_data))
        df_model_data = self.enrich_model_data(DataEnricher(df_model_data))
        
        if train:
            self.df_model_data = df_model_data
        df_x, df_y = self.transform_model_data(DataTransformer(df_model_data), train)
        return df_x, df_y
    
    
    @abstractmethod
    def define_model(self):
        pass
        
    @abstractmethod
    def train_model(self,model,df_x,df_y):
        pass
    
    @abstractmethod
    def predict_on_model(self,model,df_p_x):
        pass
    
    @abstractmethod
    def process_prediction_to_human_readable_format(self,prediction,process_prediction_object:ProcessPrediction):
        pass
    
    @abstractmethod
    def process_hrf_to_business_impact(self,df_sales, business_translator:BusinessTranslator):
        pass