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

num_data_points = 100000


# COMMAND ----------

personal_consumption_reservoir = "/reservoir/personal_consumption"

# COMMAND ----------

token = dbutils.secrets.get("defvault", "tibber-token")

# COMMAND ----------

client = TibberClient(token)

# COMMAND ----------

result = client.fetch_from_api(first=num_data_points)
data = result["data"]["viewer"]["homes"][0]

# COMMAND ----------

data

# COMMAND ----------

now = datetime.datetime.now()

# COMMAND ----------

file_name = f"pc_tibber_{now}.json"
file_path = "/".join([personal_consumption_reservoir, file_name])
print(f"{file_path=}")

# COMMAND ----------

dbutils.fs.put(file_path, json.dumps(data))

# COMMAND ----------
