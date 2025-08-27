# Databricks notebook source
storage_account_name=''
storage_account_key=''
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# =====================================================================================
# BRONZE LAYER - Raw Data Ingestion
# =====================================================================================
storage_account_name=''
storage_account_key=''
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


@dlt.table(
    name="bronze_sales",
    comment="Raw sales data ingested continuously from ADLS"
)
def bronze_sales():
    """
    Ingests raw sales data as a stream from a CSV source using Auto Loader.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("")
    )

@dlt.table(
    name="bronze_products",
    comment="Raw products data ingested continuously from ADLS"
)
def bronze_products():
    """
    Ingests raw products data as a stream from a JSON source using Auto Loader.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("")
    )

# =====================================================================================
# SILVER LAYER - Cleaned and Enriched Data
# =====================================================================================

@dlt.table(
    name="silver_sales",
    comment="Cleaned, validated, and enriched sales data"
)
@dlt.expect_or_drop("valid_sale_id", "sale_id IS NOT NULL")
@dlt.expect_or_drop("valid_sale_date", "sale_date IS NOT NULL")
@dlt.expect("valid_sales_amount", "sales_amount >= 0")
@dlt.expect("valid_quantity", "quantity > 0")
def silver_sales():
    """
    Reads the bronze sales stream, applies all cleaning and transformation logic,
    and enforces data quality rules.
    """
    df = dlt.read_stream("bronze_sales")
    df_transformed = df \
        .withColumn('sale_id', F.col('sale_id').cast('int')) \
        .withColumn('product_id', F.col('product_id').cast('int')) \
        .withColumn('region', F.col('region').cast('string')) \
        .withColumn('quantity', F.col('quantity').cast('int')) \
        .withColumn('sales_amount', F.col('sales_amount').cast('double')) \
        .withColumn('sale_date', F.col('sale_date').cast('timestamp')) \
        .withColumn("store_id", F.regexp_extract(F.col("store_id"), r"Store_(\d+)", 1).cast("int"))
    df_enriched = df_transformed \
        .withColumn("sale_month_year", (F.year(F.col("sale_date")) * 100 + F.month(F.col("sale_date"))).cast("int")) \
        .withColumn("sale_month", F.month(F.col("sale_date")).cast("int")) \
        .withColumn("sale_month_name", F.date_format(F.col("sale_date"), "MMMM")) \
        .withColumn("sale_year", F.year(F.col("sale_date")).cast("int"))
    return df_enriched.withWatermark("sale_date", "10 hours").dropDuplicates(["sale_id"])

@dlt.table(
    name="silver_products",
    comment="Cleaned and validated products data"
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_products():
    """
    Reads the bronze products stream, applies cleaning and transformation logic,
    and enforces data quality rules.
    """
    df = dlt.read_stream("bronze_products")
    df_transformed = df \
        .withColumn('category', F.col('category').cast('string')) \
        .withColumn('discount_rate', F.col('discount_rate').cast('double')) \
        .withColumn('price', F.col('price').cast('double')) \
        .withColumn('product_id', F.col('product_id').cast('int')) \
        .withColumn('product_name', F.substring(F.col("product_name"), 9, 100))
    return df_transformed.dropDuplicates(["product_id"])

# =====================================================================================
# GOLD LAYER - Business-Level Aggregates
# =====================================================================================

@dlt.table(
    name="gold_monthly_sales",
    comment="Monthly aggregated sales data per product"
)
def gold_monthly_sales():
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    return (
        sales.join(products, "product_id", "inner")
        .groupBy("sale_year", "sale_month", "sale_month_name", "product_name", "category")
        .agg(
            F.sum("sales_amount").alias("total_sales_amount"),
            F.sum("quantity").alias("total_quantity_sold")
        )
    )

@dlt.table(
    name="gold_sales_by_region",
    comment="Aggregated sales data by region and product category"
)
def gold_sales_by_region():
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    return (
        sales.join(products, "product_id", "inner")
        .groupBy("region", "category")
        .agg(
            F.sum("sales_amount").alias("total_sales_amount"),
            F.count("sale_id").alias("number_of_sales")
        )
    )

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue and quantity aggregated per region and store"
)
def gold_daily_revenue():
    sales = dlt.read("silver_sales")
    return (
        sales.groupBy(F.to_date(F.col("sale_date")).alias("sale_day"), "region", "store_id")
             .agg(
                 F.sum("sales_amount").alias("daily_revenue"),
                 F.sum("quantity").alias("total_quantity")
             )
    )

@dlt.table(
    name="gold_monthly_revenue_trend",
    comment="Monthly revenue aggregated per region"
)
def gold_monthly_revenue_trend():
    sales = dlt.read("silver_sales")
    return (
        sales.groupBy("sale_month_year", "region")
             .agg(F.sum("sales_amount").alias("monthly_revenue"))
    )

@dlt.table(
    name="gold_product_performance",
    comment="Product-level sales performance with discount insights"
)
def gold_product_performance():
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    return (
        sales.join(products, "product_id", "inner")
             .groupBy("product_id", "product_name", "category")
             .agg(
                 F.sum("sales_amount").alias("total_sales"),
                 F.sum("quantity").alias("total_quantity"),
                 F.avg("discount_rate").alias("avg_discount_rate")
             )
    )

@dlt.table(
    name="gold_top_products_by_region",
    comment="Top 5 products by total sales per region"
)
def gold_top_products_by_region():
    from pyspark.sql.window import Window
    
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    ranked = (
        sales.join(products, "product_id", "inner")
             .groupBy("region", "product_name")
             .agg(F.sum("sales_amount").alias("total_sales"))
    )
    windowSpec = Window.partitionBy("region").orderBy(F.desc("total_sales"))
    return (
        ranked.withColumn("rank", F.row_number().over(windowSpec))
              .filter(F.col("rank") <= 5)
    )

@dlt.table(
    name="gold_store_efficiency",
    comment="Store-level KPIs for benchmarking"
)
def gold_store_efficiency():
    sales = dlt.read("silver_sales")
    products = dlt.read("silver_products")
    return (
        sales.join(products, "product_id", "inner")
             .groupBy("store_id", "region")
             .agg(
                 (F.sum("sales_amount") / F.countDistinct("sale_id")).alias("avg_sales_per_transaction"),
                 F.avg("discount_rate").alias("avg_discount_applied")
             )
    )
