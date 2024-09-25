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


# COMMAND ----------

# df = spark.read.table(bronze_table)
# # 

# COMMAND ----------

# df_static = (spark.read.table(bronze_table))
# display(df_static.select('ingest_time').distinct( ))
# from delta import DeltaTable
# dt = DeltaTable.forName(spark, bronze_table)
# display(dt.detail())
# display(dt.history())

# COMMAND ----------

# display(df)

# COMMAND ----------

df = df.withColumn('consumption', F.from_json(F.col('consumption'), schema))
df = df.withColumn('nodes', F.explode(F.col('consumption.nodes')))


# COMMAND ----------

# display(df)

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

# display(df)

# COMMAND ----------

df = df.withColumn('day', F.dayofmonth(F.col('from')))
df = df.withColumn('month', F.month(F.col('from')))
df = df.withColumn('year', F.year(F.col('from')))

# COMMAND ----------

# display(df)

# COMMAND ----------

def write_to_silver(df, batch_id):
    print(f"Writing to silver table...")
    if ~df.isEmpty():
        try:
            df = df.withColumn('batch_id', F.lit(batch_id))
            dt = DeltaTable.forName(spark, silver_table)
            dt.merge(df, F.expr("dt.from == df.from and dt.to == df.to")).whenNotMatchedInsertAll()
        except pyspark.sql.utils.AnalysisException:
            print("Table may not exist. Trying to create table...")
            df.write.saveAsTable(silver_table)
    else:
        print("No data to write")

# COMMAND ----------

# write_to_silver(df, -1)

# COMMAND ----------

# display(df)

# COMMAND ----------

df.writeStream.option("checkpointLocation", "/checkpoints/silver/pc_to_silver").outputMode('append').toTable(silver_table).start()#foreachBatch(write_to_silver).trigger(processingTime='10 seconds').start()

# COMMAND ----------


