# Databricks notebook source
entity_resolution = (
    spark
    .read
    .option("inferSchema", "true")
    .load("/mnt/formula1dlionut/raw/entity_resolution")
)
product_deduplication = (
    spark
    .read
    .format("parquet")
    .load("/mnt/formula1dlionut/raw/product_deduplication")
)

# COMMAND ----------

(
    entity_resolution
    .write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ver_assignments.bronze_layer.entity_resolution")
)
(
    product_deduplication
    .write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ver_assignments.bronze_layer.product_deduplication")
)
