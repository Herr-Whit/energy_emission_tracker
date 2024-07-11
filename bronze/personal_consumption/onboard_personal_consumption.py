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

personal_consumption_reservoir = '/reservoir/personal_consumption'

# COMMAND ----------

with open('/Workspace/Repos/anton.whittaker@gmail.com/energy_emission_tracker/bronze/personal_consumption/secrets.json', 'r') as f:
  token = json.load(f)['tibber_token']


# COMMAND ----------

if True:
    dbutils.fs.rm(personal_consumption_reservoir, True)
    dbutils.fs.mkdirs(personal_consumption_reservoir)

# COMMAND ----------

client = TibberClient(token)

# COMMAND ----------

result = client.fetch_from_api()
data = result['data']['viewer']['homes'][0]

# COMMAND ----------

data

# COMMAND ----------

now = datetime.datetime.now()

# COMMAND ----------

file_name = f'pc_tibber_{now}.json'
file_path = '/'.join([personal_consumption_reservoir, file_name])
print(f"{file_path=}")

# COMMAND ----------

dbutils.fs.put(file_path, json.dumps(data))

# COMMAND ----------


