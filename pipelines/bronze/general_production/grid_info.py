import os
import datetime
import requests


class GridInfoClient:
    bnetz_url = "https://smard.api.proxy.bund.dev/app"

    headers = {
            'Content-Type': 'application/json',
        }

    def __init__(self):
        pass

    def _fetch_from_api(self, endpoint):
        
        endpoint_url = self.bnetz_url + endpoint
        # Send the request
        response = requests.get(endpoint_url, headers=self.headers)
        
        # Check for errors
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code {response.status_code}")
            print(response.text)
            raise ValueError()
    
    def get_indices(self, fltr, region, resolution):
        endpoint = f"/chart_data/{fltr}/{region}/index_{resolution}.json"
        return self._fetch_from_api(endpoint)
    

    def get_data(self, fltr, region, resolution, timestamp):
        endpoint = f"/chart_data/{fltr}/{region}/{fltr}_{region}_{resolution}_{timestamp}.json"
        return self._fetch_from_api(endpoint)
        
