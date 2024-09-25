import os
import datetime
import requests


class TibberClient:
    TIBBER_API_URL = "https://api.tibber.com/v1-beta/gql"

    def __init__(self, token, debug=False):
        # Select your transport with a defined url endpoint
        url = 'https://api.tibber.com/v1-beta/gql'

        # Define the headers, if necessary
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token  # if authentication is required
        }
        self.debug = debug

    def fetch_from_api(self, first=100, after=None, before=None):
        
        # Define the GraphQL query
        payload = {
            'query': f"""
             {{
                viewer {{
                    homes {{
                    id
                    consumption(resolution: HOURLY, first: {first}, after: {after if after else 'null'}, before: {before if before else 'null'}) {{
                        nodes {{
                        from
                        to
                        consumption
                        consumptionUnit
                        cost
                        currency
                        }}
                        pageInfo {{
                        endCursor
                        hasNextPage
                        }}
                    }}
                    }}
                }}
            }}
            """
        }
        if self.debug:
            print(payload['query'])

        # Send the request
        response = requests.post(self.TIBBER_API_URL, headers=self.headers, json=payload)
        
        # Check for errors
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code {response.status_code}")
            raise ValueError(response.text)
