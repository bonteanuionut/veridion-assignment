# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

entity_resolution = spark.table("ver_assignments.silver_layer.entity_resolution")

# COMMAND ----------

window_spec =  Window.partitionBy(
    F.col("company_name"),
    F.col("main_country_code"),
    F.col("main_region"),
    F.col("main_city_district"),
    F.col("main_city"),
    F.col("main_postcode"),
    F.col("main_street"),
    F.col("main_street_number"),
    F.col("main_latitude"),
    F.col("main_longitude")
).orderBy(F.col("last_updated_at").desc())

# COMMAND ----------

entity_resolution_rank = (
    entity_resolution.withColumn("unique_rank", F.row_number().over(window_spec))
)

# COMMAND ----------

entity_resolution_rank.count()

# COMMAND ----------

entity_resolution_rank.filter("unique_rank = 1").count()

# COMMAND ----------

entity_resolution_final = (
    entity_resolution_rank
    .filter(F.col("unique_rank") == 1)
    .drop("unique_rank")
)

# COMMAND ----------

(
    entity_resolution_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ver_assignments.gold_layer.entities")
)

# COMMAND ----------


