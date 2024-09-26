# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.table("unity.silver.personal_consumption")
display(df)


# COMMAND ----------

from delta import DeltaTable

dt = DeltaTable.forName(spark, "unity.silver.personal_consumption")
display(dt.history())

# COMMAND ----------

display(
    df.filter(df.year == 2024)
    .filter(df.month == 1)
    .select("consumption", "from", "to", "cost", "day")
    .withColumn("hour", F.hour("from"))
)

# COMMAND ----------

display(
    df.filter(df.year == 2024)
    .filter(df.month == 8)
    .select("consumption", "from", "to", "cost", "day")
    .withColumn("hour", F.hour("from"))
)

# COMMAND ----------

display(df.select(F.sum("cost")))

# COMMAND ----------

df = spark.read.table("unity.gold.personal_consumption_monthly")
display(df)
