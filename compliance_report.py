# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog sagar_cap2;
# MAGIC use schema sales_analytics;
# MAGIC create volume if not exists volumess;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS sagar_cap2.sales_analytics
COMMENT 'Database to store analytics and reports'
LOCATION '/Volumes/sagar_cap2/sales_analytics/volumess/'
""")

gold_sales = spark.table("sagar_cap2.sales_analytics.gold_monthly_sales")


compliance_report = gold_sales.withColumn(
    "flagged",
    (col("total_sales_amount") > 10000).cast("int")
)


compliance_report.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sagar_cap2.sales_analytics.compliance_report_delta")


csv_path = "/Volumes/sagar_cap2/sales_analytics/volumess/compliance_report_csv/"

compliance_report.write.option("header", True) \
    .mode("overwrite") \
    .csv(csv_path)

print("✅ Compliance report created as Delta table and CSV successfully!")


# COMMAND ----------

import pandas as pd
df=pd.read_csv('/Volumes/sagar_cap2/sales_analytics/volumess/compliance_report_csv/part-00000-tid-353165469647112325-34bb6c3c-ed05-40d9-9ae2-fa7ac8f1ba3e-9-1-c000.csv')
display(df['flagged'].value_counts())
