# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

#defining the schema
qualifying_raw_schema = StructType([StructField("qualifyId", IntegerType(), True),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)                                   
                                    ])

#Looping through all json files
input_file_path = "/mnt/formula1pkdl/raw/qualifying"                                    
all_files = dbutils.fs.ls(input_file_path)
json_files = [file.path for file in all_files if file.path.endswith(".json")]
print(json_files)

for file_path in json_files:
    print(f"Processing file: {file_path}")
    qualifying_raw_df = spark.read.json(file_path, schema=qualifying_raw_schema, multiLine=True)
    #display(qualifying_raw_df)

#renamming the columns
    qualifying_renamedcolumns_df = qualifying_raw_df.withColumnRenamed("qualifyId", "qualify_id")\
                .withColumnRenamed("raceId", "race_id")\
                .withColumnRenamed("driverId", "driver_id")\
                .withColumnRenamed("constructorId", "constructor_id")\
                .withColumn("load_date", current_timestamp())

# writing into the .parquet file
    qualifying_renamedcolumns_df.write.mode("overwrite").parquet("/mnt/formula1pkdl/raw/processed/qualifying")

#reading from .parquet file
    display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/qualifying"))

