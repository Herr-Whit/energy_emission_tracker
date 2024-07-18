# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark
from pyspark.sql.functions import *

from delta import DeltaTable

# COMMAND ----------

bronze_table = 'unity.bronze.global_production'
silver_table = 'unity.silver.global_production'

# COMMAND ----------

if False:
    spark.sql(f"DROP TABLE {silver_table}")

# COMMAND ----------

df = spark.readStream.table(bronze_table)

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.json(global_production_reservoir)
df = (df
    .withColumn('ingest_time', F.lit(datetime.datetime.now()))
    .withColumn('file_name', F.input_file_name())
    .withColumn('file_name', F.split(F.col('file_name'), '/')[3])
    .withColumn('filter_code', F.split(F.col('file_name'), '_')[0])
    .withColumn('region', F.split(F.col('file_name'), '_')[1])
    .withColumn('resolution', F.split(F.col('file_name'), '_')[2])
    .withColumn('request_timestamp', F.split(F.col('file_name'), '_')[3])
    )
display(df)
df = (df
    .withColumn('series', F.explode(F.col('series')))
    .withColumn('timestamp', F.col('series').getItem(0))
    .withColumn('production', F.col('series').getItem(1))
)

df = (
    df
    .drop('file_name')
    .drop('series')
)
display(df)
