# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------



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

now.

# COMMAND ----------

import uuid
load_identifier = str(uuid.UUID(int=int(time.time()))) 

# COMMAND ----------

table = 'unity.bronze.personal_consumption'

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
    id STRING,
    response STRING,
    creation_time TIMESTAMP
    )
    """
)

# COMMAND ----------

data = spark.createDataFrame([{'id': load_identifier, 'response': json.dumps(result), 'creation_time': datetime.datetime.now()}])

# COMMAND ----------

data.write.mode('append').saveAsTable(table)

# COMMAND ----------

display(spark.sql(f"""
          SELECT *
          FROM {table}"""))
