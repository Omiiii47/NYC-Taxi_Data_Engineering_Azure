# Databricks notebook source
storage = "nyctaxiprojectstorage"

# COMMAND ----------

bronze_path = "abfss://bronze@nyctaxiprojectstorage.dfs.core.windows.net/"
silver_path = "abfss://silver@nyctaxiprojectstorage.dfs.core.windows.net/"

display(dbutils.fs.ls(bronze_path))
display(dbutils.fs.ls(silver_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC reading csv data

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
    .option('inferSchema', True)\
    .option('header',True)\
    .load("abfss://bronze@nyctaxiprojectstorage.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

myschema = '''
    VendorID INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID BIGINT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE,
    cbd_congestion_fee DOUBLE
'''


# COMMAND ----------

df_trip = spark.read.format('parquet')\
        .schema(myschema)\
        .option('header', True)\
        .option('recursiveFileLookup', True)\
        .load("abfss://bronze@nyctaxiprojectstorage.dfs.core.windows.net/trips_2025_data")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('Zone1',split(col('Zone'),'/')[0])\
                .withColumn('Zone2',split(col('Zone'),'/')[1])

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

from pyspark.sql import functions as F

df_trip_zone = (df_trip_zone
    .withColumn('Zone1', F.trim(F.split(F.col('Zone'), '/').getItem(0)))
    .withColumn('Zone2', F.trim(F.expr("get(split(Zone, '/'), 1)"))))

display(df_trip_zone)

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
        .mode('append')\
        .option('path','abfss://silver@nyctaxiprojectstorage.dfs.core.windows.net/trip_zone')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip data

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date', to_date('lpep_pickup_datetime'))\
        .withColumn('trip_year', year('lpep_pickup_datetime'))\
        .withColumn('trip_month', month('lpep_pickup_datetime'))\
        .withColumn('trip_day', dayofmonth('lpep_pickup_datetime'))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('VendorID' , 'PULocationID' , 'DOLocationID' , 'fare_amount' ,'total_amount' )

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
        .mode('append')\
        .option('path','abfss://silver@nyctaxiprojectstorage.dfs.core.windows.net/trips2025data')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

display(df_trip)

# COMMAND ----------

