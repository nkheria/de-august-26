# Databricks notebook source
# MAGIC %sql
# MAGIC -- Data Engineers: Full control on Bronze & Silver tables
# MAGIC GRANT ALL PRIVILEGES ON TABLE sagar_cap2.sales_analytics.bronze_products TO data_engineer_group90;
# MAGIC GRANT ALL PRIVILEGES ON TABLE sagar_cap2.sales_analytics.silver_products TO data_engineer_group90;
# MAGIC
# MAGIC -- Analysts: Read-only on Gold table
# MAGIC GRANT SELECT ON TABLE sagar_cap2.sales_analytics.gold_top_products_by_region TO analysts_90;
# MAGIC
# MAGIC -- Auditors: Read-only on Gold tables
# MAGIC GRANT SELECT ON TABLE sagar_cap2.sales_analytics.gold_daily_revenue TO auditors_90;
# MAGIC GRANT SELECT ON TABLE sagar_cap2.sales_analytics.gold_monthly_sales TO auditors_90;