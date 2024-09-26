# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark
from pyspark.sql.functions import *

from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.dropdown('process_mode', 'streaming', ['streaming', 'batch'])
process_mode = dbutils.widgets.get('process_mode')

# COMMAND ----------

bronze_table = 'unity.bronze.general_production'
silver_table = 'unity.silver.general_production'

checkpoint_path = "/checkpoints/silver/gp_to_silver"

# COMMAND ----------

if process_mode == 'streaming':
    df = spark.readStream.table(bronze_table)
elif process_mode == 'batch':
    df = spark.table(bronze_table)
else:
    raise ValueError('Invalid process mode')

# COMMAND ----------

df.withColumn('series', F.explode(F.col('series')))

# COMMAND ----------

df = (df
    .withColumn('series', F.explode(F.col('series').cast(T.ArrayType(T.ArrayType(T.FloatType())))))
    .withColumn('timestamp', F.col('series').getItem(0).cast(T.LongType()))
    .withColumn('production', F.col('series').getItem(1).cast(T.DoubleType()))
)
df = (
    df
    .drop('series')
)

# COMMAND ----------

index_match = """table.filter_code == data.filter_code 
    and table.region == data.region
    and table.resolution == data.resolution
    and table.timestamp == data.timestamp"""
ingest_time_match = """table.ingest_time < data.ingest_time"""

# COMMAND ----------

def write_to_silver(df, batch_id):
    print(f"Writing to silver table...")
    if ~df.isEmpty():
        try:
            df = df.withColumn('batch_id', F.lit(batch_id))
            dt = DeltaTable.forName(spark, silver_table).alias('table')
            print('Looking for records to update...')
            dt.merge(df.alias('data'), F.expr(f"{index_match} and {ingest_time_match}")).whenMatchedUpdateAll().execute()
            print('Looking for records to insert...')
            dt.merge(df.alias('data'), F.expr(index_match)).whenNotMatchedInsertAll().execute()
        except pyspark.sql.utils.AnalysisException:
            print("Table may not exist. Trying to create table...")
            df.write.format('delta').partitionBy('filter_code', 'region').saveAsTable(silver_table)
    else:
        print("No data to write")

# COMMAND ----------

if process_mode == 'streaming':
    print("Starting streaming job...")
    df.writeStream.option("checkpointLocation", checkpoint_path).foreachBatch(write_to_silver).trigger(processingTime='10 seconds').start()
elif process_mode == 'batch':
    print("Starting batch job...")
    write_to_silver(df, -1)
else:
    raise ValueError('Invalid process mode')
