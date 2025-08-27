# Databricks notebook source
storage_account_name=
storage_account_key=
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sagar_cap2.sales_analytics.bronze_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sagar_cap2.sales_analytics.bronze_products
# MAGIC TIMESTAMP AS OF '2025-08-26T15:08:40.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY sagar_cap2.sales_analytics.silver_sales;
# MAGIC
# MAGIC
# MAGIC INSERT INTO sagar_cap2.sales_analytics.silver_sales
# MAGIC SELECT *
# MAGIC FROM sagar_cap2.sales_analytics.silver_sales VERSION AS OF 2 
# MAGIC WHERE sale_id = '12345'; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog sagar_recover_catalog;
# MAGIC create schema if not exists sales_analytics;