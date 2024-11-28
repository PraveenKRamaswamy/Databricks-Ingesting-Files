# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp
#defining the schema
constructors_raw_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

#reading the .json file
constructors_raw_df = spark.read.schema(constructors_raw_schema).json("/mnt/formula1pkdl/raw/constructors.json")

#Renaming the columns
constructors_raw_renamed_df = constructors_raw_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumnRenamed("name", "constructor_name").withColumnRenamed("nationality", "constructor_nationality").withColumnRenamed("url", "constructor_url").withColumn("load_date", current_timestamp())

#Dropping the column
constructors_raw_droppedcol_df = constructors_raw_renamed_df.drop(col("constructor_url"))

#Writing into a .PARQUET File
constructors_raw_droppedcol_df.write.mode("overwrite").parquet("/mnt/formula1pkdl/raw/processed/constructors")

#reading the .PARQUET File
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/constructors")





# COMMAND ----------



# COMMAND ----------

constructors_raw_df.printSchema()