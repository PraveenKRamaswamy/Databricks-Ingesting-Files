# Databricks notebook source
display(dbutils.fs.mounts())
display(dbutils.fs.ls('/mnt/formula1pkdl/raw')  )

# COMMAND ----------

## Defining the Data Types and createing the raw data frame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, concat, lit,current_timestamp
race_raw_schema = StructType([StructField("raceId", IntegerType(), False),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", IntegerType(), True),
                                 StructField("circuitId", IntegerType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("date", DateType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("url", StringType(), True)])
race_raw_df = spark.read.csv("dbfs:/mnt/formula1pkdl/raw/races.csv", header=True, inferSchema=True, schema=race_raw_schema)

#Selecting only the required columns
#circuits_df_raw_selected = circuits_df_raw.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

race_raw_select_df = race_raw_df.select(col("raceId").alias("race_Id"), col("year").alias("race_year"), col("round").alias("race_round"), col("circuitId").alias("race_circuitId"), col("name").alias("race_name"), col("date").alias("race_date"), col("time").alias("race_time"))

#CAST race_date to STRING and concat with race_time
race_raw_select_cast_df = race_raw_select_df.withColumn("race_timestamp",concat(race_raw_select_df["race_date"].cast("string"),lit(""), race_raw_select_df["race_time"])) \
.withColumn("load_date", current_timestamp())
#display(race_raw_select_cast_df) 

#Writing the dataframe to .PARQUET File
race_raw_select_cast_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1pkdl/raw/processed/races")

#Reading the .PARQUET File
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/races"))
                                                    







                                

