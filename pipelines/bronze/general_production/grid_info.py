import os
import datetime
import requests

import time
import random


class ExponentialBackoff:
    """
    A context manager to request data from an API using an exponential backoff policy.
    Retries on any 5xx errors, with exponentially increasing wait times.
    """

    def __init__(self, sleep_time=2, max_retries=5, jitter=True):
        """
        :param sleep_time: Initial wait time before retrying (in seconds)
        :param max_retries: Maximum number of retries
        :param jitter: Whether to add a random jitter to avoid thundering herd problems
        """
        self.sleep_time = sleep_time
        self.max_retries = max_retries
        self.jitter = jitter
        self.current_retry = 0

    def __enter__(self):
        # This is called when the context block starts, we don't need to do anything specific here.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Called when an exception is raised in the context block
        if exc_type is not None and self.current_retry < self.max_retries:
            if exc_type.__name__.startswith('HTTP') and exc_val.response.status_code >= 500:
                # Check if the error is a 5xx error
                self.current_retry += 1
                wait_time = self.sleep_time * (2 ** (self.current_retry - 1))
                if self.jitter:
                    # Add a bit of randomness to prevent all retries happening at the same time
                    wait_time += random.uniform(0, 1)

                print(f"Retry {self.current_retry}/{self.max_retries}. Waiting {wait_time:.2f} seconds.")
                time.sleep(wait_time)

                # Suppress the exception by returning True, so the context block can retry
                return True
            else:
                # For any other error, don't suppress the exception, return False to propagate it
                return False

        # If no exception, or retries exceeded, return False to exit normally
        return False


class GridInfoClient:
    bnetz_url = "https://smard.api.proxy.bund.dev/app"

    headers = {
            'Content-Type': 'application/json',
        }

    def __init__(self):
        pass

    def _fetch_from_api(self, endpoint):
        
        endpoint_url = self.bnetz_url + endpoint

        with ExponentialBackoff(sleep_time=1, max_retries=5) as backoff:
            while backoff.current_retry < backoff.max_retries:
                try:
                    # Simulate an API request
                    response = requests.get(endpoint_url, headers=self.headers)
                    response.raise_for_status()  # Raises HTTPError for bad responses (like 5xx errors)
                    # If successful, break out of the retry loop
                    print("Success!", response.content)
                    break
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code >= 500:
                        print("Encountered 5xx error. Retrying...")
                        # The `__exit__` method will handle the backoff and retry
                        continue
                    else:
                        raise  # Re-raise non-5xx errors
        
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
        
