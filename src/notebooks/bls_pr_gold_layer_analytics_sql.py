# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Setup: use the gold schema from databricks.yml
import yaml

with open('../../databricks.yml') as f:
    bundle_vars = yaml.safe_load(f)['variables']

CATALOG = bundle_vars['catalog']['default']
GOLD_SCHEMA = bundle_vars['gold_schema']['default']

spark.sql(f'USE `{CATALOG}`.`{GOLD_SCHEMA}`')
print(f'Using {CATALOG}.{GOLD_SCHEMA}')

# COMMAND ----------

# DBTITLE 1,Q1: US population mean and std dev, 2013-2018
# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)                     AS n_years,
# MAGIC   min(d.year)                  AS first_year,
# MAGIC   max(d.year)                  AS last_year,
# MAGIC   avg(f.population)            AS mean_population,
# MAGIC   stddev_samp(f.population)    AS stddev_sample,
# MAGIC   stddev_pop(f.population)     AS stddev_population
# MAGIC FROM fact_population_annual f
# MAGIC JOIN dim_date d
# MAGIC   ON f.date_key = d.date_key AND d.is_annual
# MAGIC JOIN dim_geography g
# MAGIC   ON f.geography_id = g.geography_id
# MAGIC WHERE g.geography_name = 'United States'
# MAGIC   AND d.year BETWEEN 2013 AND 2018

# COMMAND ----------

# DBTITLE 1,Q2: Year with largest summed quarterly value per series
# MAGIC %sql
# MAGIC WITH yearly AS (
# MAGIC   SELECT series_id, year, sum(CAST(value AS DECIMAL(18,3))) AS total_value, count(*) AS quarters_in_year
# MAGIC   FROM fact_productivity_quarterly
# MAGIC   WHERE value IS NOT NULL
# MAGIC   GROUP BY series_id, year
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *,
# MAGIC          rank()   OVER (PARTITION BY series_id ORDER BY total_value DESC) AS rk,
# MAGIC          count(*) OVER (PARTITION BY series_id, total_value)              AS same_total
# MAGIC   FROM yearly
# MAGIC )
# MAGIC SELECT r.series_id,
# MAGIC        s.series_title,
# MAGIC        r.year              AS best_year,
# MAGIC        r.total_value,
# MAGIC        r.quarters_in_year,
# MAGIC        r.same_total > 1    AS is_tie
# MAGIC FROM ranked r
# MAGIC JOIN dim_series s USING (series_id)
# MAGIC WHERE r.rk = 1
# MAGIC ORDER BY r.series_id, r.year

# COMMAND ----------

# DBTITLE 1,Q3: PRS30006032 Q01 value per year, joined with population
# MAGIC %sql
# MAGIC SELECT o.series_id,
# MAGIC        'Q01'        AS period,
# MAGIC        o.year,
# MAGIC        o.value,
# MAGIC        p.population
# MAGIC FROM fact_productivity_quarterly o
# MAGIC LEFT JOIN fact_population_annual p
# MAGIC   ON p.date_key = o.year * 10
# MAGIC  AND p.geography_id = (SELECT geography_id FROM dim_geography WHERE geography_name = 'United States')
# MAGIC WHERE o.series_id = 'PRS30006032'
# MAGIC   AND o.quarter = 1
# MAGIC ORDER BY o.year