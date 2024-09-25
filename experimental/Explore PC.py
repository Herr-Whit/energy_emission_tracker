# Databricks notebook source
import pyspark.sql.functions as F   

# COMMAND ----------

df = spark.read.table('unity.silver.personal_consumption')

# COMMAND ----------

display(df.filter(df.year == 2024).filter(df.month == 1).select('consumption', 'from', 'to', 'cost', 'day').withColumn('hour', F.hour('from')))

# COMMAND ----------

df = spark.read.table('unity.gold.personal_consumption_monthly')
display(df)
