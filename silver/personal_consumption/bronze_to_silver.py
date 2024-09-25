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

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {silver_table}
          """)

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
    if ~df.isEmpty():
        try:
            display(df)
            df = df.withColumn('batch_id', F.lit(batch_id))
            dt = DeltaTable.forName(spark, silver_table)
            dt.merge(df, F.expr("dt.from == df.from and dt.to == df.to")).whenNotMatchedInsert()
        except pyspark.sql.utils.AnalysisException:
            df.write.saveAsTable(silver_table)

# COMMAND ----------

df.writeStream.foreachBatch(write_to_silver).start()

# COMMAND ----------


