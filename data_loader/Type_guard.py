import pandas as pd
    
def integer_guard(dataframe : pd.Series):
    assert pd.api.types.is_integer_dtype(dataframe), f"{dataframe.columns} is not of integer type"

def string_guard(dataframe : pd.Series): 
    assert pd.api.types.is_string_dtype(dataframe ), f"{dataframe.columns} is not of string type"
                
def datetime_guard(dataframe : pd.Series):
    assert pd.api.types.is_datetime64_any_dtype(dataframe) , f"{dataframe.columns} is not of datetime64 type"
    
def object_guard(dataframe : pd.Series):
    assert pd.api.types.is_object_dtype(dataframe), f"{dataframe.columns} is not of object type"

def float_guard(dataframe : pd.Series):
    assert pd.api.types.is_float_dtype(dataframe), f"{dataframe.columns} is not of float64 type"

# def integer_guard_bool(dataframe : pd.Series):
#     return pd.api.types.is_integer_dtype(dataframe)

# def string_guard_bool(dataframe : pd.Series): 
#     return dataframe != "Test"
                
# def datetime_guard_bool(dataframe : pd.Series):
#     return pd.api.types.is_datetime64_any_dtype(dataframe) 
    
# def object_guard_bool(dataframe : pd.Series):
#     return pd.api.types.is_object_dtype(dataframe)

# def float_guard_bool(dataframe : pd.Series):
#     return pd.api.types.is_any_real_numeric_dtype(dataframe)
