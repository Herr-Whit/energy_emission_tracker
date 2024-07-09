# Databricks notebook source
import pyspark
import pyspark.sql.functions as F

import json
import time
import datetime


# COMMAND ----------

table = 'unity.bronze.personal_consumption'
personal_consumption_reservoir = '/reservior/personal_consumption'


# COMMAND ----------

stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema("value STRING").load(personal_consumption_reservoir)

# COMMAND ----------

stream.w

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
