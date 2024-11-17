# Databricks notebook source
dbutils.notebook.run('teardown_all', timeout_seconds=120)

# COMMAND ----------

dbutils.notebook.run('setup_all', timeout_seconds=120)
