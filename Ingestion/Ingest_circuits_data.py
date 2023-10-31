# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True),

])

# COMMAND ----------

circuits_df = spark.read.option('Header', True) \
.schema(circuits_schema) \
.csv("/mnt/formula1learning/raw/circuits.csv") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop the 'URL' coloumn which is not required

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), 
                                     col("circuitRef"), 
                                     col("name"),
                                     col("location"),
                                     col("country"),
                                     col("lat"),
                                     col("lng"),
                                     col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column names

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('name', 'track_name') \
    .withColumnRenamed('location', 'track_location') \
    .withColumnRenamed('country', 'country') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final DF with ingestion date column added

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output data into parquet file

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet("/mnt/formula1learning/processed/circuits")
