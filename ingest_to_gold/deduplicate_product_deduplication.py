# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

product_deduplication = spark.table("ver_assignments.silver_layer.product_deduplication")

# COMMAND ----------

window_spec = Window.partitionBy(
    F.col("root_domain"),
    F.col("product_title"),
    F.col("product_name"),
    F.col("product_identifier"),
    F.col("eco_friendly"),
    F.col("energy_efficiency")
).orderBy(
    F.desc(F.size(F.col("materials"))),
    F.desc(F.size(F.col("intended_industries"))),
    F.desc(F.size(F.col("ethical_and_sustainability_practices"))),
    F.desc(F.size(F.col("production_capacity"))),
    F.desc(F.size(F.col("price"))),
    F.desc(F.size(F.col("materials"))),
    F.desc(F.size(F.col("ingredients"))),
    F.desc(F.size(F.col("manufacturing_countries"))),
    F.desc(F.size(F.col("manufacturing_type"))),
    F.desc(F.size(F.col("customization"))),
    F.desc(F.size(F.col("packaging_type"))),
    F.desc(F.size(F.col("form"))),
    F.desc(F.size(F.col("size"))),
    F.desc(F.size(F.col("color"))),
    F.desc(F.size(F.col("purity"))),
    F.desc(F.size(F.col("pressure_rating"))),
    F.desc(F.size(F.col("power_rating"))),
    F.desc(F.size(F.col("quality_standards_and_certifications"))),
    F.desc(F.size(F.col("miscellaneous_features"))),
)

# COMMAND ----------

product_deduplication_rank = (
    product_deduplication.withColumn("unique_rank", F.row_number().over(window_spec))
)

# COMMAND ----------

product_deduplication_rank_filtered = product_deduplication_rank.filter(F.col("unique_rank") == 1)

# COMMAND ----------


