# Databricks notebook source
df = spark.read.table('unity.silver.personal_consumption')

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.table('unity.gold.personal_consumption_monthly')
display(df)
