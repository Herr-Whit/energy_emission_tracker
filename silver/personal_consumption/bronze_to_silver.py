# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark
from pyspark.sql.functions import *

from delta import DeltaTable

# COMMAND ----------

bronze_table = 'unity.bronze.personal_consumption'
silver_table = 'unity.silver.personal_consumption'

# COMMAND ----------

inner_schema = T.StructType([
    T.StructField("from", T.TimestampType(), True),
    T.StructField("to", T.TimestampType(), True),
    T.StructField("consumption", T.DoubleType(), True),
    T.StructField("consumptionUnit", T.StringType(), True),
    T.StructField("cost", T.DoubleType(), True),
    T.StructField("currency", T.StringType(), True)
])

# Define the outer schema containing an array of the inner schema
schema = T.StructType([
    T.StructField("nodes", T.ArrayType(inner_schema), True)
])

# COMMAND ----------

df = spark.readStream.table(bronze_table)
# df = spark.read.table(bronze_table)


# COMMAND ----------

df = df.withColumn('consumption', F.from_json(F.col('consumption'), schema))
df = df.withColumn('nodes', F.explode(F.col('consumption.nodes')))


# COMMAND ----------

df = (
    df
    .withColumn('from', F.col('nodes.from'))
    .withColumn('to', F.col('nodes.to'))
    .withColumn('consumption', F.col('nodes.consumption'))
    .withColumn('consumptionUnit', F.col('nodes.consumptionUnit'))
    .withColumn('cost', F.col('nodes.cost'))
    .withColumn('currency', F.col('nodes.currency'))
    .drop('nodes')
)

# COMMAND ----------

df = df.withColumn('day', F.dayofmonth(F.col('from')))
df = df.withColumn('month', F.month(F.col('from')))
df = df.withColumn('year', F.year(F.col('from')))

# COMMAND ----------

def write_to_silver(df, batch_id):
    print(f"Writing to silver table...")
    if ~df.isEmpty():
        try:
            df = df.withColumn('batch_id', F.lit(batch_id))
            dt = DeltaTable.forName(spark, silver_table).alias('dt')
            dt.merge(df.alias('df'), F.expr("dt.from == df.from and dt.to == df.to")).whenNotMatchedInsertAll().execute()
        except pyspark.sql.utils.AnalysisException:
            print("Table may not exist. Trying to create table...")
            df.write.format('delta').saveAsTable(silver_table)
    else:
        print("No data to write")

# COMMAND ----------

# df.writeStream.option("checkpointLocation", "/checkpoints/silver/pc_to_silver").outputMode('append').toTable(silver_table).start()#foreachBatch(write_to_silver).trigger(processingTime='10 seconds').start()
df.writeStream.option("checkpointLocation", "/checkpoints/silver/pc_to_silver").foreachBatch(write_to_silver).trigger(processingTime='10 seconds').start()

# COMMAND ----------


