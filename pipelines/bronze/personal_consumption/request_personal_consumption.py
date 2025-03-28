# Databricks notebook source
# MAGIC %md # Request Data from Tibber
# MAGIC - For now do this in one batch for 100.000 hours (a bit more than 11 years)
# MAGIC - Potential improvement would be taking a delta from what's onboarded
# MAGIC - Save it to a reservoir to be picked up by _cloudfiles_ stream and processed to bronze layer

# COMMAND ----------

import pyspark
import pyspark.sql.functions as F

import json
import time
import datetime

from tibber_client import TibberClient

num_data_points = 1000000


# COMMAND ----------

personal_consumption_reservoir = "/reservoir/personal_consumption"

# COMMAND ----------

token = dbutils.secrets.get("defvault", "tibber-token")

# COMMAND ----------

client = TibberClient(token, debug=True)

# COMMAND ----------

result = client.fetch_from_api(first=num_data_points)
data = result["data"]["viewer"]["homes"][0]
num_data_retrieved = len(data['consumption']['nodes'])
print(f"{num_data_retrieved=}")

now = datetime.datetime.now()
file_name = f"pc_tibber_{now}.json"
file_path = "/".join([personal_consumption_reservoir, file_name])
print(f"{file_path=}")
dbutils.fs.put(file_path, json.dumps(data))

while result["data"]["viewer"]["homes"][0]["consumption"]["pageInfo"]["hasNextPage"]:
    print("next page...")
    nextpage_cursor = result["data"]["viewer"]["homes"][0]["consumption"]["pageInfo"]["endCursor"]
    print(f"{nextpage_cursor=}")
    result = client.fetch_from_api(first=num_data_points, after=nextpage_cursor)
    data = result["data"]["viewer"]["homes"][0]
    num_data_retrieved += len(result["data"]["viewer"]["homes"][0]["consumption"]["nodes"])
    print(f"{num_data_retrieved=}")
    print(f"first date: {result['data']['viewer']['homes'][0]['consumption']['nodes'][0]['from']}")

    now = datetime.datetime.now()
    file_name = f"pc_tibber_{now}.json"
    file_path = "/".join([personal_consumption_reservoir, file_name])
    print(f"{file_path=}")
    dbutils.fs.put(file_path, json.dumps(data))


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


