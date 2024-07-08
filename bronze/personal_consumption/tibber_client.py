import os
import datetime

import pandas as pd
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport



class TibberClient:
    TIBBER_API_URL = "https://api.tibber.com/v1-beta/gql"

    def __init__(self, token):
        # Select your transport with a defined url endpoint
        transport = AIOHTTPTransport(
            url=self.TIBBER_API_URL,
            headers={"Authorization": f"Bearer {token}"},
        )

        # Create a GraphQL client using the defined transport
        self.client = Client(transport=transport, fetch_schema_from_transport=True)


    def fetch_from_api(self):
        query = gql(
            """
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
        )
        result = self.client.execute(query)
        data = result["viewer"]["homes"][0]["currentSubscription"]["priceInfo"]
        df = self.wrangle_tibber_data(data)
        if datetime.datetime.now().hour >= 13 and df.shape[0] == 48:
            if not os.path.exists("data"):
                os.makedirs("data")
            df.to_csv(self.get_current_file_path(), index=False)
        return df
