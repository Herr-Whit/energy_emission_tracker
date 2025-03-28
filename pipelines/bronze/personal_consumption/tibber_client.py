import os
import datetime
import requests


class TibberClient:
    """
    A client to interact with Tibbers graphQL API in order to retrieve the personal consumption data for a single home.
    """

    TIBBER_API_URL = "https://api.tibber.com/v1-beta/gql"

    def __init__(self, token, debug=False):
        # Select your transport with a defined url endpoint
        url = "https://api.tibber.com/v1-beta/gql"

        # Define the headers
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
        }

        # Set debug mode for more detailed logging of intermediate results
        self.debug = debug

    def fetch_from_api(self, first=100, after=None, before=None):

        # Define the GraphQL query to retieve the consumption data (from a given time OR to a given point in time; and the number of hours to retrieve)
        after = f'"{after}"' if after else 'null'
        before = f'"{before}"' if before else 'null'
        payload = {
            "query": f"""
             {{
                viewer {{
                    homes {{
                    id
                    consumption(resolution: HOURLY, first: {first}, after: {after}, before: {before}) {{
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
            print(payload["query"])

        # Send the request
        response = requests.post(
            self.TIBBER_API_URL, headers=self.headers, json=payload
        )

        # Check for errors
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code {response.status_code}")
            raise ValueError(response.text)
