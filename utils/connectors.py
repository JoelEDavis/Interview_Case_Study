import os
import logging
import requests
from datetime import datetime
import pandas as pd
import json
import time
from typing import Dict, Any, Optional, Union, List

class API_Client():
    def __init__(self, base_url: str):
        """
        Connect to FastAPI to extract and load data.
        
        Args:
            base_url (str): The base URL of the API endpoints.
        """
        self.base_url = base_url.strip("/")
        self.session = requests.Session()

        self.headers = {"Content-Type": "application/json"}
        self.session.headers.update(self.headers)

    def get(self, endpoint: str):
        """
        Send a GET request to a specified endpoint.

        Args:
            endpoint (str): Targeted API endpoint to get data

        Returns:
            JSON response data 
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, data: any):
        """
        Send a POST request to a specified endpoint.

        Args:
            endpoint (str): Targeted API endpoint to post data 

        Returns:
            JSON response data 
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        success_count = 0
        failure_count = 0
        responses = []
        failed_records = []

        if isinstance(data, pd.DataFrame):
                total_records = len(data)
                print(f"Starting migration of {total_records} records to {endpoint}")
                
                if total_records > 0:
                    for index, row in data.iterrows():
                        record = row.to_dict()
                        try:
                            response = self.session.post(
                                url,
                                json=record,
                                headers={"Content-Type": "application/json"}
                            )
                            response.raise_for_status()
                            responses.append(response.json())
                            success_count += 1
                            
                            if success_count % 10 == 0 or success_count == total_records:
                                print(f"Progress: {success_count}/{total_records} records migrated successfully")
                            
                        except requests.exceptions.RequestException as e:
                            failure_count += 1
                            failed_records.append({"record": record, "error": str(e)})
                            print(f"Error posting record {index}: {e}")
                            if hasattr(e.response, "text"):
                                print(f"Response: {e.response.text}")
                            continue
                    
                    print(f"Migration complete: {success_count} successes, {failure_count} failures")
                else:
                    print("Warning: Empty DataFrame provided to post method")
            
        else:
            try:
                response = self.session.post(
                    url,
                    json=data,
                    headers={"Content-Type": "application/json"}
                )
                response.raise_for_status()
                responses.append(response.json())
                success_count = 1
                print(f"Successfully posted 1 record to {endpoint}")
                
            except requests.exceptions.RequestException as e:
                failure_count = 1
                failed_records.append({"record": data, "error": str(e)})
                print(f"Error posting to {url}: {e}")
                if hasattr(e.response, "text"):
                    print(f"Response: {e.response.text}")
            
        return {
            "success_count": success_count,
            "failure_count": failure_count,
            "responses": responses,
            "failed_records": failed_records
        }

