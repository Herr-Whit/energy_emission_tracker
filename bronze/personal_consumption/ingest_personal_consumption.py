# Databricks notebook source
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

if True:
    spark.sql(
    f"""
    DROP TABLE {table}
    """
    )
    dbutils.fs.rm(checkpoint_location, True)

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

dbutils.fs.ls(personal_consumption_reservoir)

# COMMAND ----------

# dbutils.fs.head('/reservoir/personal_consumption/pc_tibber_2024-07-11 14:42:44.529871.json')

# COMMAND ----------

stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(schema).load(personal_consumption_reservoir)

# COMMAND ----------

stream = stream.withColumn('ingest_time', F.lit(datetime.datetime.now()))

# COMMAND ----------

query = (stream.writeStream
         .outputMode('append')
         .option("checkpointLocation", checkpoint_location)
         .toTable(table))

query.awaitTermination()
