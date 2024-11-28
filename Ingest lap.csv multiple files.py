# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp
#defining the schema
laptime_raw_schema = StructType([StructField("laptime_race_id", IntegerType(), True),
                                 StructField("laptime_driver_id", IntegerType(), True),
                                 StructField("laptime_lap_number", IntegerType(), True),
                                 StructField("laptime_position", IntegerType(), True),
                                 StructField("laptime_time", StringType(), True),
                                 StructField("laptime_milliseconds", IntegerType(), True )
                                ])

#Reading all laptime split files
laptime_raw_df = spark.read.csv("dbfs:/mnt/formula1pkdl/raw/lap_times/lap_times_split_*.csv", schema=laptime_raw_schema)
#display(laptime_raw_df)

#renaming columns
laptime_renamedcolumn_df = laptime_raw_df.withColumn("load_date", current_timestamp())


#writing into a .parquet file
laptime_renamedcolumn_df.write.mode("overwrite").parquet("/mnt/formula1pkdl/raw/processed/lap_times")

#reading from .parquet file
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/lap_times"))