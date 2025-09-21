-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.table("ver_assignments.bronze_layer.entity_resolution").printSchema()

-- COMMAND ----------

SELECT * FROM ver_assignments.bronze_layer.entity_resolution
LIMIT 40

-- COMMAND ----------

SELECT DISTINCT to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') FROM ver_assignments.bronze_layer.entity_resolution

-- COMMAND ----------

SELECT 
  main_country_code,
  main_country,
  main_region,
  main_city_district,
  main_city,
  main_postcode,
  main_street,
  main_street_number,
  main_latitude,
  main_longitude,
  locations,
  split(locations, ',') as splitted_locations
FROM
  ver_assignments.bronze_layer.entity_resolution
WHERE locations IS NOT NULL
  AND (
    main_country_code IS NULL
    OR main_country IS NULL
    OR main_region IS NULL
    OR main_city_district IS NULL
    OR main_city IS NULL
    OR main_postcode IS NULL
    OR main_street IS NULL
    OR main_street_number IS NULL
    OR main_latitude IS NULL
    OR main_longitude IS NULL
  )


-- COMMAND ----------

SELECT locations, split(locations, ' \\| ') as split_locations FROM
  ver_assignments.bronze_layer.entity_resolution
WHERE num_locations == '1'

-- COMMAND ----------

SELECT
  coalesce(company_name, company_legal_names, company_commercial_names) as company_name,
  main_country_code,
  main_region,
  main_city_district,
  main_city,
  main_postcode,
  main_street,
  main_street_number,
  COUNT(*) as counts
FROM ver_assignments.bronze_layer.entity_resolution
GROUP BY
  coalesce(company_name, company_legal_names, company_commercial_names),
  main_country_code,
  main_region,
  main_city_district,
  main_city,
  main_postcode,
  main_street,
  main_street_number
HAVING COUNT(*) > 1
ORDER BY counts DESC

-- COMMAND ----------

SELECT
  coalesce(company_name, company_legal_names, company_commercial_names) as company_name,
  main_latitude,
  main_longitude,
  COUNT(*) as counts
FROM ver_assignments.bronze_layer.entity_resolution
GROUP BY
  coalesce(company_name, company_legal_names, company_commercial_names),
  main_latitude,
  main_longitude
HAVING COUNT(*) > 1
ORDER BY counts DESC

-- COMMAND ----------

SELECT * FROM ver_assignments.bronze_layer.entity_resolution
WHERE coalesce(company_name, company_legal_names, company_commercial_names) IS NULL

-- COMMAND ----------


