# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sagar_cap2.raw_data.sales_raw;
# MAGIC DROP TABLE IF EXISTS sagar_cap2.raw_data.products_raw;

# COMMAND ----------

#Migration setup
# Set up configs for Destination Storage
spark.conf.set(

)
#copying file from my one container to another for sales
# Define paths
source_path =
dest_path   =

# Read data from source (all files)
df = spark.read.format("csv").load(source_path)

# Write data into destination
(
    df.write
    .format("csv")
    .mode("overwrite")
    .save(dest_path)
)

print("✅ Migration of all sales files completed successfully!")

# COMMAND ----------


#copying file from my one container to another for products
# Set up configs for Destination Storage
spark.conf.set(

)

# Define paths
source_path =
dest_path   =

# Read data from source (all files), ignoring corrupt files
df = (
    spark.read
    .format("json")
    .option("ignoreCorruptFiles", "true")
    .load(source_path)
)

# Write data into destination
(
    df.write
    .format("json")
    .mode("overwrite")
    .select('*').drop('_corrupt_record').write.format('json').mode('overwrite').save(dest_path)
)

print("✅ Migration of all files completed successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop if table already exists
# MAGIC DROP TABLE IF EXISTS sagar_cap2.raw_data.sales_raw;
# MAGIC
# MAGIC -- Create Sales Raw Table (CSV files)
# MAGIC CREATE TABLE sagar_cap2.raw_data.sales_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   inferSchema = "true"
# MAGIC )
# MAGIC LOCATION '';
# MAGIC
# MAGIC -- Drop if table already exists
# MAGIC DROP TABLE IF EXISTS sagar_cap2.raw_data.products_raw;
# MAGIC
# MAGIC -- Create Products Raw Table (JSON files)
# MAGIC CREATE TABLE sagar_cap2.raw_data.products_raw
# MAGIC USING JSON
# MAGIC OPTIONS (
# MAGIC   multiline = "true"
# MAGIC )
# MAGIC LOCATION '';

# COMMAND ----------

