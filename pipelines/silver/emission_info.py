# Databricks notebook source
import pyspark.sql.functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.dropdown("processing_mode", "streaming", ["batch", "streaming"])
processing_mode = dbutils.widgets.get("processing_mode")

# COMMAND ----------

bronze_table = "unity.bronze.emissions"
silver_table = "unity.silver.emissions"
checkpoint_path = "dbfs:/checkpoints/dev_bronze_emissions/"

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
    powerplant_id STRING,
    powerplant_name STRING,
    g_per_kwh DOUBLE,
    ingest_time TIMESTAMP,
    valid_from DATE,
    valid_to DATE,
    is_active BOOLEAN,
    batch_id INT
    )
    """
)

# COMMAND ----------

if processing_mode == "streaming":
    df = (
    spark.readStream
    .table(bronze_table)
    )
elif processing_mode == "batch":
    df = spark.read.table(bronze_table)
else:
    raise ValueError("Invalid processing mode")

# COMMAND ----------

df = (
    df.withColumn("ingest_time", F.current_timestamp())
    .withColumn("is_active", F.lit(True))
    .withColumn(
        "valid_from", F.lit(F.current_date()))
    .withColumn('valid_to', F.lit('9999-01-01'))
    .withColumn("valid_to", F.col("valid_to").cast("date"))
)

# COMMAND ----------

def update_scd_2_emissions(df, batchId):
    df = df.withColumn("batch_id", F.lit(batchId))
    dt = DeltaTable.forName(spark, silver_table)
    dt.alias("table").merge(df.alias("df"), "False").whenMatchedUpdate(
        set={
        "table.is_active": "False",
        "table.valid_to": F.lit(F.current_date()),
        }
    ).whenNotMatchedInsertAll().execute()

# COMMAND ----------

if processing_mode == "batch":
    update_scd_2_emissions(df, -1)
elif processing_mode == "streaming":
    df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(silver_table)
else:
    raise Exception("Invalid processing mode")

# COMMAND ----------


