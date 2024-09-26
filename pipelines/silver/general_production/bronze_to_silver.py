# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark
from pyspark.sql.functions import *

from delta import DeltaTable

# COMMAND ----------

bronze_table = 'unity.bronze.general_production'
silver_table = 'unity.silver.general_production'

# COMMAND ----------

if False:
    spark.sql(f"DROP TABLE {silver_table}")

# COMMAND ----------

df = spark.read.table(bronze_table)

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumn('series', F.explode(F.col('series')))

# COMMAND ----------

df = (df
    .withColumn('series', F.explode(F.col('series').cast(T.ArrayType(T.ArrayType(T.FloatType())))))
    .withColumn('timestamp', F.col('series').getItem(0).cast(T.IntegerType()))
    .withColumn('production', F.col('series').getItem(1).cast(T.DoubleType()))
)

df = (
    df
    .drop('series')
)
display(df)

# COMMAND ----------

df.write.mode('append').saveAsTable(silver_table)

# COMMAND ----------


