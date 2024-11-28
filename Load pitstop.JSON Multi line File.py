# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

#Defining the Schema
pitstop_raw_schema = StructType([StructField("raceId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("stop", StringType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("duration", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True)
                                 ])

#Reading the multiline JSON File
pitstop_raw_df = spark.read.schema(pitstop_raw_schema).json("/mnt/formula1pkdl/raw/pit_stops.json", schema=pitstop_raw_schema,multiLine=True)

#Renaming the columns
pitstop_renamedcolumns_df = pitstop_raw_df.withColumnRenamed("raceId", "pitstop_race_id").withColumnRenamed("driverId", "pitstop_driver_id").withColumnRenamed("stop", "pitstop_stop").withColumnRenamed("lap", "pitstop_lap").withColumnRenamed("time", "pitstop_time").withColumnRenamed("duration", "pitstop_duration").withColumnRenamed("milliseconds", "pitstop_milliseconds").withColumn("load_date", current_timestamp())

#Writng to the .PARQUET File
pitstop_renamedcolumns_df.write.mode("overwrite").parquet("/mnt/formula1pkdl/raw/processed/pit_stops")

#Reading the .PARQUET File
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/pit_stops"))

