# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

#Defining the schema for the raw data, the name is nested object
name_schema = StructType([StructField("forename", StringType(), True), 
                          StructField("surname", StringType(), True)])

driver_schema = StructType([StructField("driverId", IntegerType(), True),
                            StructField("driverRef", StringType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("code", StringType(), True),
                            StructField("name", name_schema, True),
                            StructField("dob", DateType(), True),
                            StructField("nationality", StringType(), True),
                            StructField("url", StringType(), True)
                            ])
#Reading json file
drivers_raw_df = spark.read.json("/mnt/formula1pkdl/raw/drivers.json")

#Rename the columns
drivers_raw_renamedcolumn_df = drivers_raw_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
        .withColumnRenamed("number", "driver_number")\
            .withColumnRenamed("code", "driver_code")\
                .withColumn("driver_name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                    .withColumnRenamed("dob", "driver_dob")\
                        .withColumnRenamed("nationality", "driver_nationality")\
                        .withColumn("load_date", current_timestamp())
                                    
#Dropping the columns
drivers_raw_dropcolumn_df = drivers_raw_renamedcolumn_df.drop("name", "url")

#Writing into the .PARQUET File
drivers_raw_dropcolumn_df.write.mode("overwrite").parquet("/mnt/formula1pkdl/raw/processed/drivers")

#Reading the .PARQUET File
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/drivers"))