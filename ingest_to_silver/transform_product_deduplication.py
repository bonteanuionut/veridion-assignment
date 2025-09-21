# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

product_duplicates = spark.table("ver_assignments.bronze_layer.product_deduplication")

# COMMAND ----------

for c in product_duplicates.dtypes:
    if "array" in c[1]:
        product_duplicates = (
            product_duplicates
            .withColumn(
                c[0],
                F.when(F.col(c[0]) == F.array(), F.lit(None)).otherwise(F.col(c[0]))
            )
            .withColumn(
                c[0], F.sort_array(F.col(c[0]))
            )
        )

# COMMAND ----------

total_records = product_duplicates.count()
diff_nulls = [total_records - product_duplicates.filter((F.col(col).isNull())).count() for col in product_duplicates.columns]

# COMMAND ----------

zip_nulls = list(zip(list(product_duplicates.columns), diff_nulls))
zip_columns_nulls = [x[0] for x in filter(lambda x: x[-1] == 0, zip_nulls)]

# COMMAND ----------

product_duplicates_drop_columns = (
    product_duplicates.drop(*zip_columns_nulls)
)

# COMMAND ----------

(
    product_duplicates_drop_columns
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("ver_assignments.silver_layer.product_deduplication")
)

# COMMAND ----------


