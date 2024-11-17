# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.table("unity.gold.general_emissions")
display(df.withColumn('hr_timestamp', (F.col("timestamp") / 1000).cast("timestamp")).select('filter_code', 'production', 'hr_timestamp', 'emission_per_source'))


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
