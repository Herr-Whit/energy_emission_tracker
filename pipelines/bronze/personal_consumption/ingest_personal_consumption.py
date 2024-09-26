# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Personal Consumption Data
# MAGIC Stream the requested json files to bronze

# COMMAND ----------

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

import json
import time
import datetime


# COMMAND ----------

table = 'unity.bronze.personal_consumption'
personal_consumption_reservoir = '/reservoir/personal_consumption'
checkpoint_location = "dbfs:/checkpoints/dev_bronze_pc/" 


# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
    consumption STRING,
    ingest_time TIMESTAMP
    )
    """
)

# COMMAND ----------

schema = T.StructType([
    T.StructField("consumption", T.StringType(), True)
])

# COMMAND ----------

stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(schema).load(personal_consumption_reservoir)

# COMMAND ----------

stream = stream.withColumn('ingest_time', F.current_timestamp())

# COMMAND ----------

query = (stream.writeStream
         .outputMode('append')
         .option("checkpointLocation", checkpoint_location)
         .toTable(table))

query.start()
