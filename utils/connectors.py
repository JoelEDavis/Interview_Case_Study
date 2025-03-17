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
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()


