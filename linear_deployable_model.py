from data_loader.clean_transaction_data import DataCleaner
from data_loader.enrich_transaction_data import DataEnricher
from data_loader.transform_transaction_data import DataTransformer
from prediction_handeler.process_prediction import ProcessPrediction
from prediction_handeler.predicted_sales_impact_uploader import BusinessTranslator

from abc_deployable_model import DeployableModel

import pandas as pd
from datetime import timedelta
from sklearn.preprocessing import PolynomialFeatures, RobustScaler, MinMaxScaler, OneHotEncoder, KBinsDiscretizer, OrdinalEncoder
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.compose import ColumnTransformer


import logging

class DumyModel(DeployableModel):
    
    def deploy(self):
        df_x, df_y = self.transform_df_model_data_to_df_x_df_y(self.df_model_data, True)
        df_p_x, df_p_y = self.transform_df_model_data_to_df_x_df_y(self.df_p_model_data, False)
        
        model = self.define_model()
        model = self.train_model(model,df_x,df_y)
        prediction = self.predict_on_model(model,df_p_x)
        # use instead of df_x, due to the encoding in the data transformer messing up the indexes. 
        
        df_prediction_hrf = self.process_prediction_to_human_readable_format(ProcessPrediction(prediction),df_p_y, df_y)
        self.process_hrf_to_business_impact(BusinessTranslator(df_prediction_hrf))
    
    def clean_model_data(self, cleaner_object:DataCleaner) -> pd.DataFrame:
        cleaner_object.remove_unstocked_products()
        cleaner_object.remove_products_with_no_recent_sales(False, timedelta(days=65))
        return cleaner_object.df_transactions
    
    def enrich_model_data(self, enricher_object:DataEnricher) -> pd.DataFrame:
        weather_properties = ["tavg","prcp"]
        enricher_object.add_weather_data(weather_properties)
        enricher_object.add_time_feature_column("weekday")
        return enricher_object.df_transactions
    
    @staticmethod
    def define_data_transformer():
        """scale training data to be effectively used for linear regression
        
        The principle that for each location there should be a separate linear regression
        is achieved in this data manipulation by increasing the dimensionality of the onehot 
        with the location and weekday.  
        """
        weather_impact = make_pipeline(KNNImputer(), RobustScaler())
        column_transformer = ColumnTransformer(transformers=[
                                ("robustscaler",weather_impact, ["tavg","prcp"]),
                                ("onehot",OneHotEncoder(), ["weekday"]),
                                ], remainder="drop")

        return column_transformer
    
    def transform_model_data(self, transformer_object:DataTransformer, train:bool) -> tuple[pd.DataFrame, pd.DataFrame]:
        df_x, df_y = transformer_object.frequency_encode()
        
        if train:
            # define data transformer as self as it needs to be reused in the prediction phase
            self.data_transformer = self.define_data_transformer().fit(df_x.reset_index())
        # for the prediction data it should not be fitted     
        x_enc = self.data_transformer.transform(df_x.reset_index()) # no naming and indexes
        return x_enc, df_y
    
    def define_model(self):
        return lr_model
    
    def train_model(self, model, df_x, df_y):
        logging.info("Model is starting training")
        model.fit(X = df_x, y= df_y)
        logging.info("Model is trained")
        return model
    
    def predict_on_model(self,model,df_p_x):
        logging.info("Model is starting prediction")
        prediction = model.predict(df_p_x)
        logging.info("Model has predicted")
        return prediction
    
    def process_prediction_to_human_readable_format(self,process_prediction_object:ProcessPrediction, df_with_indexes,df_with_columns):
        process_prediction_object.replace_negative_predictions()
        df_prediction = process_prediction_object.create_dataframe(df_with_indexes,df_with_columns)
        df_sales = process_prediction_object.prediction_to_cumulative_sales()
        return df_sales
        
    def process_hrf_to_business_impact(self,business_translator:BusinessTranslator):
        business_translator.process_sales_to_business_impact()

if (__name__ == "__main__"):
    logging.basicConfig(filename='log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        LinearModel().deploy()
        logging.info("Model has been deployed")
    except:
        logging.exception('Error occured:')