# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    TimestampType,
    DoubleType,
    ArrayType
)

# COMMAND ----------

entity_resolution = spark.table("ver_assignments.bronze_layer.entity_resolution")

# COMMAND ----------

entity_resolution_split = (
    entity_resolution
    .withColumn("locations", F.split(entity_resolution["locations"], " \\| "))
    .withColumn("locations_data_splitted", F.split(F.col("locations").getItem(0), ","))
    .withColumn("locations_data_splitted_size", F.size("locations_data_splitted"))
    .withColumn("phone_numbers", F.split(F.col("phone_numbers"), " \\| "))
    .withColumn("emails", F.split(F.col("emails"), " \\| "))
    .withColumn("sic_codes", F.split(F.col("sic_codes"), " \\| "))
    .withColumn("sic_labels", F.split(F.col("sic_labels"), " \\| "))
    .withColumn("isic_v4_codes", F.split(F.col("isic_v4_codes"), " \\| "))
    .withColumn("isic_v4_labels", F.split(F.col("isic_v4_labels"), " \\| "))
    .withColumn("nace_rev2_codes", F.split(F.col("nace_rev2_codes"), " \\| "))
    .withColumn("nace_rev2_labels", F.split(F.col("nace_rev2_labels"), " \\| "))
    .withColumn("generated_business_tags", F.split(F.col("generated_business_tags"), " \\| "))
    .withColumn("domains", F.split(F.col("domains"), " \\| "))
    .withColumn("all_domains", F.split(F.col("all_domains"), " \\| "))
)

# COMMAND ----------

entity_resolution_transformed = (
    entity_resolution_split
    .withColumn("main_country_code", F.coalesce(F.col("main_country_code"), F.col("locations_data_splitted").getItem(0)))
    .withColumn("main_country", F.coalesce(F.col("main_country"), F.col("locations_data_splitted").getItem(1)))
    .withColumn("main_region", F.coalesce(F.col("main_region"), F.col("locations_data_splitted").getItem(2)))
    .withColumn("main_latitude", F.coalesce(F.col("main_latitude"), F.col("locations_data_splitted").getItem(-2)))
    .withColumn("main_longitude", F.coalesce(F.col("main_longitude"), F.col("locations_data_splitted").getItem(-1)))
)

# COMMAND ----------

entity_resolution_drop_columns = (
    entity_resolution_transformed
    .drop("locations_data_splitted", "locations_data_splitted_size")
)

# COMMAND ----------

entity_resolution_final = (
    entity_resolution_transformed
    .withColumn("year_founded", F.col("year_founded").cast(IntegerType()))
    .withColumn("lnk_year_founded", F.col("lnk_year_founded").cast(IntegerType()))
    .withColumn("naics_2022_primary_code", F.col("naics_2022_primary_code").cast(IntegerType()))
    .withColumn("num_locations", F.col("num_locations").cast(IntegerType()))
    .withColumn("main_latitude", F.col("main_latitude").cast(DoubleType()))
    .withColumn("main_longitude", F.col("main_longitude").cast(DoubleType()))
    .withColumn("website_number_of_pages", F.col("website_number_of_pages").cast(IntegerType()))
    .withColumn("employee_count", F.col("employee_count").cast(IntegerType()))
    .withColumn("inbound_links_count", F.col("inbound_links_count").cast(IntegerType()))
    .withColumn("revenue", F.col("revenue").cast(DoubleType()))
    .withColumn("created_at", F.to_timestamp(F.col("created_at")))
    .withColumn("last_updated_at", F.to_timestamp(F.col("last_updated_at")))
)

# COMMAND ----------

(
    entity_resolution_final
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("ver_assignments.silver_layer.entity_resolution")
)

# COMMAND ----------


