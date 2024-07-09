# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import pyspark
import pyspark.sql.functions as F

import json
import time
import datetime

from tibber_client import TibberClient


# COMMAND ----------

with open('/Workspace/Repos/anton.whittaker@gmail.com/energy_emission_tracker/bronze/personal_consumption/secrets.json', 'r') as f:
  token = json.load(f)['tibber_token']


# COMMAND ----------

client = TibberClient(token)

# COMMAND ----------

result = client.fetch_from_api()

# COMMAND ----------

now = datetime.datetime.now()

# COMMAND ----------

personal_consumption_reservoir = '/reservior/personal_consumption'
file_name = f'pc_tibber_{now}.json'
file_path = '/'.join([personal_consumption_reservoir, file_name])
print(f"{file_path=}")

# COMMAND ----------

dbutils.fs.put(file_path, json.dumps(result))
