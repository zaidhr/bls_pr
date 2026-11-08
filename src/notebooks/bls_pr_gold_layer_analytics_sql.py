# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Setup: use the gold schema from databricks.yml
import yaml

# Catalog/schema come from the bundle variable defaults in <repo>/databricks.yml
with open('../../databricks.yml') as f:
    bundle_vars = yaml.safe_load(f)['variables']

CATALOG = bundle_vars['catalog']['default']
GOLD_SCHEMA = bundle_vars['gold_schema']['default']

# All queries below use unqualified gold table names
spark.sql(f'USE `{CATALOG}`.`{GOLD_SCHEMA}`')
print(f'Using {CATALOG}.{GOLD_SCHEMA}')

# COMMAND ----------

# DBTITLE 1,Q1: US population mean and std dev, 2013-2018
# MAGIC %sql
# MAGIC -- Sample std dev (n-1) is the usual choice; population std dev (n) shown for reference
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
# MAGIC -- Sum of Q01-Q04 values per series and year, then the top year per series.
# MAGIC -- RANK keeps ties (is_tie = true); quarters_in_year < 4 flags partial years (e.g. the current year).
# MAGIC WITH yearly AS (
# MAGIC   SELECT series_id, year, sum(value) AS total_value, count(*) AS quarters_in_year
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
# MAGIC -- Series PRS30006032, period Q01 (quarter 1 of the quarterly fact).
# MAGIC -- Population is annual, so it joins on the year; LEFT JOIN keeps years without population (NULL).
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