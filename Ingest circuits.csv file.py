# Databricks notebook source
# MAGIC %md
# MAGIC **Read the circuits.csv file from the raw folder and display in a dataframe and applying the correct Data Types**
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

circuits_file_path_raw = "dbfs:/mnt/formula1pkdl/raw/circuits.csv"
print(circuits_file_path_raw)

circuits_file_schema = StructType([StructField("circuitId", IntegerType(), False), 
                                   StructField("circuitRef", StringType(), True), 
                                   StructField("name", StringType(), True), 
                                   StructField("location", StringType(), True), 
                                   StructField("country", StringType(), True), 
                                   StructField("lat", DoubleType(), True), 
                                   StructField("lng", DoubleType(), True),
                                   StructField("alt", IntegerType(), True),
                                   StructField("url", StringType(), True)]
                                  )
#print(circuits_file_schema)

circuits_df_raw = spark.read.csv(circuits_file_path_raw, header=True, inferSchema = True, schema = circuits_file_schema)
#display(circuits_df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC **# Select only the Required Columns**

# COMMAND ----------

circuits_df_raw_selected = circuits_df_raw.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
#display(circuits_df_raw_selected)

# from pyspark.sql.functions import col
# circuits_df_raw_selected = circuits_df_raw.select(col("circuitId").alias("circuitId"), col("circuitRef").alias("circuitRef"), col("name").alias("name"), col("location").alias("location"), col("country").alias("country"), col("lat").alias("latitude"), col("lng").alias("lng"), col("alt").alias("alt"))
# display(circuits_df_raw_selected)

# COMMAND ----------

#Renaming the Columns
circuits_df_raw_renamed = (
    circuits_df_raw_selected
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("name", "circuit_name") \
    .withColumnRenamed("location", "circuit_location") \
    .withColumnRenamed("country", "circuit_country") \
    .withColumnRenamed("lat", "circuit_latitude") \
    .withColumnRenamed("lng", "circuit_longtitude") \
    .withColumnRenamed("alt", "circuit_altitue")
)
# display(circuits_df_raw_renamed)

from pyspark.sql.functions import current_timestamp
circuits_df_raw_final = circuits_df_raw_renamed.withColumn("load_date", current_timestamp())
#display(circuits_df_raw_final)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write the Dataframe into a .PARQUET FILE in the Data `Lake`**

# COMMAND ----------

#circuits_df_raw_final.write.mode("overwrite").parquet("/mnt/formula1pkdl/raw/processed/circuits")
display(spark.read.parquet("/mnt/formula1pkdl/raw/processed/circuits"))