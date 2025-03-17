from utils.connectors import API_Client
from utils.transformations import remove_invalid_rows, convert_to_dataframe, convert_currency, annual_aggregation
from datetime import datetime
import pandas as pd

base_url = "https://technical-case-platform-engineer.onrender.com"

exchange_rate_endpoint = "exchange-rates"
monthly_date_endpoint = "monthly-data"
annual_data_endpoint = "annual-data"

today = datetime.today()

if __name__ == "__main__":
    api_connector = API_Client(base_url)
    
    # Get data from API
    currency_data = api_connector.get(exchange_rate_endpoint)
    print(f"Currency data extracted...")
    value_data = api_connector.get(monthly_date_endpoint)
    print(f"Monthly data extracted...")

    # Convert to dataframes
    currency_df = convert_to_dataframe(currency_data)
    value_df = convert_to_dataframe(value_data)
    
    # Process data
    cleaned_value_df = remove_invalid_rows(value_df, currency_df)
    print("Invalid rows removed...")
    converted_df = convert_currency(cleaned_value_df, currency_df)
    print("Currencies converted...")
    annual_agg = annual_aggregation(converted_df)
    print("Annual aggregations produced...")

    # Post the annual aggregation data
    response = api_connector.post(annual_data_endpoint, annual_agg)
    
    # Print response confirmation
    print(f"Data posted successfully!")