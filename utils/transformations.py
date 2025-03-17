import pandas as pd
from datetime import datetime

def remove_invalid_rows(data: pd.DataFrame, currency_data: pd.DataFrame) -> pd.DataFrame:
    """
    Process and remove invalid rows based on following parameters:
        1. Timestamp must be in the format YYYY-MM-DD and before today's date
        2. Value of company must not be negative
        3. Only entries with currencies in the exchange-rates endpoint are valid
    
    Args:
        data (pd.DataFrame): Input DataFrame to clean
        currency_list (list): List of valid currency codes
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with invalid rows removed
    """
    df = data.copy()
    currency_df = currency_data.copy()

    valid_currencies = set(currency_df["from_currency"]).union(set(currency_df["to_currency"]))
    
    valid_timestamp_mask = df["timestamp"].apply(lambda x: 
        isinstance(x, str) and 
        is_valid_date_format(x) and 
        datetime.strptime(x, '%Y-%m-%d').date() < datetime.now().date()
    )
    
    valid_value_mask = df["value"].apply(lambda x: 
        not pd.isna(x) and float(x) >= 0
    )
    
    valid_currency_mask = df["currency"].apply(lambda x: 
        not pd.isna(x) and x in valid_currencies
    )
    
    valid_rows_mask = valid_timestamp_mask & valid_value_mask & valid_currency_mask
    
    return df[valid_rows_mask]

def is_valid_date_format(date_str):
    """
    Check if a string is in YYYY-MM-DD format
    """
    if pd.isna(date_str):
        return False
    
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
def convert_to_dataframe(data: any):
    """
    Take the JSON output from the endpoint and convert to a dataframe for analysis.
        
    Args:
        data: JSON data

    Returns:
        pandas.DataFrame: DataFrame containing data
    """
    if isinstance(data, list):
        df = pd.DataFrame(data)

    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list) and len(value)> 0:
                df = pd.DataFrame(value)
                break

            else:
                df = pd.DataFrame([data])

    else: df = pd.DataFrame()
    
    return df

def convert_currency(data: pd.DataFrame, currency_data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert non-SEK currencies to SEK to normalise valuations.

    Args:
        data (pd.DataFrame): DataFrame containing monthly company data
        currency_date (pd.DataFrame): DataFrame containing currency pairs and their conversion rates

    Returns:
        pd.DataFrame: DataFrame containing monthly company data with converted values
    """
    data_df = data.copy()
    currency_df = currency_data.copy()

    for i, row in data_df.iterrows():
        currency = row["currency"]
        matching_rate = currency_df[currency_df["from_currency"] == currency]

        if not matching_rate.empty:
            conversion = matching_rate.iloc[0]
            data_df.at[i, "value"] = row["value"] * conversion["rate"]
            data_df.at[i, "currency"] = conversion["to_currency"]

    return data_df

def annual_aggregation(data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate monthly data into annual, per-company output.

    Args:
        data (pd.DataFrame): DataFrame with columns timestamp, value, company and currency.

    Returns:
        pd.DataFrame: DataFrame containing companies aggregated annually by valuations.
    """
    data_df = data.copy()

    data_df["timestamp"] = pd.to_datetime(data_df["timestamp"])
    data_df["year"] = data_df["timestamp"].dt.year

    annual_aggregation = data_df.groupby(["company", "year", "currency"]).agg(
        value = ("value", "mean")
    ).reset_index()

    return annual_aggregation