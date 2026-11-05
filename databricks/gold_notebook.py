# Databricks notebook source
# MAGIC %md
# MAGIC ### Delta reading and writing and creating delta tables

# COMMAND ----------

silver_path = "abfss://silver@nyctaxiprojectstorage.dfs.core.windows.net/"
gold_path = "abfss://gold@nyctaxiprojectstorage.dfs.core.windows.net/"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_zone = spark.read.format('parquet')\
    .option('header', 'true')\
    .option('interSchema', 'true')\
    .load(silver_path + 'trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS nyc_catalog
# MAGIC MANAGED LOCATION 'abfss://gold@nyctaxiprojectstorage.dfs.core.windows.net/managed/';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_catalog.gold;

# COMMAND ----------

df_zone.write.format('delta')\
    .mode('overwrite')\
    .option('path',f'{gold_path}/trip_zone')\
    .saveAsTable('nyc_catalog.gold.trip_zone')

# COMMAND ----------

df_trips = spark.read.format('parquet')\
    .option('header', 'true')\
    .option('interSchema', 'true')\
    .load(silver_path + 'trips2025data')

# COMMAND ----------

df_trips.write.format('delta')\
    .mode('overwrite')\
    .option('path',f'{gold_path}/trip_data')\
    .saveAsTable('nyc_catalog.gold.trip_2025_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc_catalog.gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nyc_catalog.gold.trip_2025_data

# COMMAND ----------

