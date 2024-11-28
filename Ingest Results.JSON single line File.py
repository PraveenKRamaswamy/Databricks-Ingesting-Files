# Databricks notebook source
from pyspark.sql.functions import current_timestamp

#Defining the schema
results_raw_schema = "resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, laps INT, time STRING, milliseconds STRING, fastestLap INT, rank INT, fastestLapTime TIMESTAMP, fastestLapSpeed INT, statusId INT"

#Reading the JSON File
results_raw_df = spark.read.json("/mnt/formula1pkdl/raw/results.json", schema = results_raw_schema)

#Renaming the Columns
results_renamedcolumns_df = results_raw_df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "result_race_id").withColumnRenamed("driverId", "result_driverid").withColumnRenamed("constructorId", "result_constructorid").withColumnRenamed("number", "result_number").withColumnRenamed("grid", "result_grid").withColumnRenamed("position", "result_position").withColumnRenamed("positionText", "result_position_text").withColumnRenamed("positionOrder", "result_position_order").withColumnRenamed("laps", "result_laps").withColumnRenamed("time", "result_time").withColumnRenamed("milliseconds", "result_milliseconds").withColumnRenamed("fastestLap", "result_fastest_lap").withColumnRenamed("rank", "result_rank").withColumnRenamed("fastestLapTime", "result_fastest_lap_time").withColumnRenamed("fastestLapSpeed", "result_fastest_lap_speed").withColumnRenamed("statusId", "result_status_id").withColumn("load_date" ,current_timestamp())

display(results_renamedcolumns_df)

#Writing to .PARQUET File
results_renamedcolumns_df.write.mode("overwrite").partitionBy("result_race_id").parquet("/mnt/formula1pkdl/raw/processed/results")

#Reading from the .PARQUET FILE
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/results"))