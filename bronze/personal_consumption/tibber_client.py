import os
import datetime
import requests


class TibberClient:
    TIBBER_API_URL = "https://api.tibber.com/v1-beta/gql"

    def __init__(self, token):
        # Select your transport with a defined url endpoint
        url = 'https://api.tibber.com/v1-beta/gql'

        # Define the headers, if necessary
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token  # if authentication is required
        }

    def fetch_from_api(self):
        
        # Define the GraphQL query or mutation
        query = """
             {
                viewer {
                    homes {
                    id
                    consumption(resolution: HOURLY, first: 100, after: null, before: null) {
                        nodes {
                        from
                        to
                        consumption
                        consumptionUnit
                        cost
                        currency
                        }
                        pageInfo {
                        endCursor
                        hasNextPage
                        }
                    }
                    }
                }
            }
        """

        # Define the payload
        payload = {
            'query': query
        }

        # Send the request
        response = requests.post(self.TIBBER_API_URL, headers=self.headers, json=payload)
        
        # Check for errors
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code {response.status_code}")
            print(response.text)
            raise ValueError()
