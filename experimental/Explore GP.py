# Databricks notebook source
import pyspark.sql.functions as F   

# COMMAND ----------

silver_table = 'unity.silver.general_production'

# COMMAND ----------

df = spark.read.table()
print(df.count())
display(df.select('ingest_time').distinct())


# COMMAND ----------

from delta import DeltaTable
dt = DeltaTable.forName(spark, silver_table)
display(dt.history())

# COMMAND ----------

df = spark.read.table('unity.bronze.general_production')
print(df.count())
display(df.select('ingest_time').distinct())


# COMMAND ----------

from delta import DeltaTable
dt = DeltaTable.forName(spark, 'unity.silver.personal_consumption')
display(dt.history())

# COMMAND ----------

display(df.filter(df.year == 2024).filter(df.month == 1).select('consumption', 'from', 'to', 'cost', 'day').withColumn('hour', F.hour('from')))

# COMMAND ----------

display(df.filter(df.year == 2024).filter(df.month == 8).select('consumption', 'from', 'to', 'cost', 'day').withColumn('hour', F.hour('from')))

# COMMAND ----------

display(df.select(F.sum('cost')))

# COMMAND ----------

df = spark.read.table('unity.gold.personal_consumption_monthly')
display(df)
