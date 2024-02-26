from azure_connectors.AzureSqlCommunicator import get_query_from_file, execute_query_and_load_results_into_dataframe,connect_azure
import pandas as pd
import datetime
import logging


def load_product_average_sales_per_day() -> pd.DataFrame:
    """Returns average sales per day per productid
    Output:
        index: [productid, weekday]
        values: average sales
    """
    connection = connect_azure()
    avg_daily_sales = get_query_from_file("sql/average_daily_sales.sql")
    df_average_sales_per_day = execute_query_and_load_results_into_dataframe(connection,  avg_daily_sales)
    df_average_sales_per_day.dropna(inplace=True, axis=0)
    df_average_sales_per_day['ProductId'] = df_average_sales_per_day['ProductId'].astype('int64')
    df_average_sales_per_day.set_index(['Location','ProductId'], inplace=True)
    
    
    return df_average_sales_per_day



def select_how_many_days_to_average_over(df_predicted_sales, lookahead_days):
    """
    Selects how many days to average over for predicted sales.

    Parameters:
    - df_predicted_sales (pandas.DataFrame): DataFrame containing predicted sales data.
    - lookahead_days (int): Number of days to look ahead for averaging.

    Returns:
    - pandas.DataFrame: DataFrame with average sales per day.
    """
    
    end_of_week = (datetime.datetime.now() + datetime.timedelta(days= 1 + lookahead_days)).strftime("%Y-%m-%d")
    
    return pd.DataFrame(df_predicted_sales[end_of_week].rename("GemiddeldVerkochtPerDag")).div(lookahead_days)

def create_sales_next_week(df_average_sales_per_day:pd.DataFrame):
    """
    Create sales data for the next week based on the average sales per day.

    Args:
        df_average_sales_per_day (pd.DataFrame): DataFrame containing the average sales per day.

    Returns:
        pd.DataFrame: DataFrame containing the sales data for the next week.
    """
    daily_sales_next_week = df_average_sales_per_day.copy()
    
    start = (datetime.datetime.now() + datetime.timedelta(days=1)).date()
    end = (datetime.datetime.now() + datetime.timedelta(days=7)).date()
    
    dates_next_week = pd.date_range(start=start, end=end, freq='D')
    
    daily_sales_next_week[dates_next_week] = \
    df_average_sales_per_day["GemiddeldVerkochtPerDag"].apply(
        lambda x: [x*1, x*2, x*3, x*4, x*5, x*6, x*7]
    ).tolist()

    daily_sales_next_week.drop(columns=["GemiddeldVerkochtPerDag"], inplace=True)
    daily_sales_next_week = daily_sales_next_week.round(0).astype(int)
    return daily_sales_next_week


def replace_statistical_outliers(df_predicted_sales:pd.DataFrame, 
                                 standard_deviations_off = 1) -> pd.DataFrame:
    """Automaticly replace predictions that are more than x standard diviations of the original model
    
    The predicted sales are imported as this has to be updated, 
    and is intened to be used in the process_prediction.py file. where this is a already used variable
    
    the general idea is in 3 steps:
        - process and the data into the same format(df_predicted_sales)
        - compare statistics and differences
        - update the predictions based on the outliers
    
    """
    df_predicted_average_sales = select_how_many_days_to_average_over(df_predicted_sales,3)
    # process the data into a workable format
    df_average_sales_per_day = load_product_average_sales_per_day()
    df_averge_sales_next_week = create_sales_next_week(df_average_sales_per_day)
    
    # compare statistics and differences
    df_difference_sales_and_prediction = df_predicted_average_sales.\
        subtract(df_average_sales_per_day).dropna()

    standard_deviations = df_difference_sales_and_prediction.std()

    outliers_mask = (df_difference_sales_and_prediction.abs() > 
                    1 * standard_deviations).values
    outliers_index = df_difference_sales_and_prediction[outliers_mask].index
    
    logging.info(f"Outlier products found and adjusted: {(len(outliers_index))/len(df_average_sales_per_day)}")

    # update the predictions based on the outliers
    df_predicted_sales.update(df_averge_sales_next_week.loc[outliers_index], join= "left")
    
    return df_predicted_sales