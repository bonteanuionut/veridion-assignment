-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.table("ver_assignments.bronze_layer.product_deduplication").printSchema()

-- COMMAND ----------

SELECT * FROM ver_assignments.bronze_layer.product_deduplication
WHERE product_title = 'C-6 FR Deluxe Guitar'
LIMIT 20

-- COMMAND ----------

SELECT DISTINCT 
  quality_standards_and_certifications,
  dense_rank() over(order by size(quality_standards_and_certifications) desc) as rnk,
  count(*)
FROM ver_assignments.bronze_layer.product_deduplication
group by 1
having count(*) > 1

-- COMMAND ----------

SELECT DISTINCT manufacturing_year
FROM ver_assignments.bronze_layer.product_deduplication

-- COMMAND ----------

SELECT * FROM ver_assignments.bronze_layer.product_deduplication
LIMIT 20

-- COMMAND ----------

SELECT DISTINCT 
  product_identifier
FROM ver_assignments.bronze_layer.product_deduplication

-- COMMAND ----------

select
  product_title,
  product_name,
  count(*) as counts
from ver_assignments.bronze_layer.product_deduplication
group by product_title, product_name
having count(*) > 1
order by counts desc

-- COMMAND ----------

select
  product_title,
  product_name,
  unspsc,
  count(*) as counts
from ver_assignments.bronze_layer.product_deduplication
group by product_title, product_name, unspsc
having count(*) > 1
order by counts desc

-- COMMAND ----------

select
  product_title,
  product_name,
  unspsc,
  description,
  count(*) as counts
from ver_assignments.bronze_layer.product_deduplication
group by product_title, product_name, unspsc, description
having count(*) > 1
order by counts desc

-- COMMAND ----------

select
  product_identifier,
  product_title,
  product_name,
  count(*) as counts
from ver_assignments.bronze_layer.product_deduplication
group by product_title, product_name, unspsc, product_identifier
having count(*) > 1
order by counts desc

-- COMMAND ----------

select
  product_identifier,
  root_domain,
  product_title,
  product_name,
  count(*) as counts
from ver_assignments.bronze_layer.product_deduplication
group by product_identifier,
  root_domain,
  product_title,
  product_name
having count(*) > 1
order by counts desc

-- COMMAND ----------

SELECT * FROM ver_assignments.bronze_layer.product_deduplication
WHERE product_title = 'C-6 FR Deluxe Guitar'

-- COMMAND ----------


