# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Setup: gold schema from databricks.yml
import yaml
from pyspark.sql import Window
from pyspark.sql import functions as F

# Catalog/schema come from the bundle variable defaults in <repo>/databricks.yml
with open('../../databricks.yml') as f:
    bundle_vars = yaml.safe_load(f)['variables']

GOLD = f'{bundle_vars["catalog"]["default"]}.{bundle_vars["gold_schema"]["default"]}'

fact_population = spark.table(f'{GOLD}.fact_population_annual')
fact_quarterly = spark.table(f'{GOLD}.fact_productivity_quarterly')
dim_date = spark.table(f'{GOLD}.dim_date')
dim_geography = spark.table(f'{GOLD}.dim_geography')
dim_series = spark.table(f'{GOLD}.dim_series')

# US population per year (annual date rows only), reused by Q1 and Q3
us_population = (
    fact_population
    .join(dim_date.where('is_annual').select('date_key'), 'date_key')
    .join(dim_geography.where(F.col('geography_name') == 'United States').select('geography_id'), 'geography_id')
    .select('year', 'population')
)
print(f'Using {GOLD}')

# COMMAND ----------

# DBTITLE 1,Q1: US population mean and std dev, 2013-2018
# Sample std dev (n-1) is the usual choice; population std dev (n) shown for reference
q1 = (
    us_population
    .where(F.col('year').between(2013, 2018))
    .agg(
        F.count('*').alias('n_years'),
        F.min('year').alias('first_year'),
        F.max('year').alias('last_year'),
        F.avg('population').alias('mean_population'),
        F.stddev_samp('population').alias('stddev_sample'),
        F.stddev_pop('population').alias('stddev_population'),
    )
)
display(q1)

# COMMAND ----------

# DBTITLE 1,Q2: Year with largest summed quarterly value per series
# Sum of Q01-Q04 values per series and year, then the top year per series.
# rank() keeps ties (is_tie = true); quarters_in_year < 4 flags partial years (e.g. the current year).
yearly = (
    fact_quarterly
    .where(F.col('value').isNotNull())
    .groupBy('series_id', 'year')
    .agg(F.sum('value').alias('total_value'), F.count('*').alias('quarters_in_year'))
)

by_total = Window.partitionBy('series_id').orderBy(F.col('total_value').desc())
same_total = Window.partitionBy('series_id', 'total_value')

q2 = (
    yearly
    .withColumn('rk', F.rank().over(by_total))
    .withColumn('same_total', F.count('*').over(same_total))
    .where(F.col('rk') == 1)
    .join(dim_series.select('series_id', 'series_title'), 'series_id')
    .select(
        'series_id',
        'series_title',
        F.col('year').alias('best_year'),
        'total_value',
        'quarters_in_year',
        (F.col('same_total') > 1).alias('is_tie'),
    )
    .orderBy('series_id', 'best_year')
)
display(q2)

# COMMAND ----------

# DBTITLE 1,Q3: PRS30006032 Q01 value per year, joined with population
# Series PRS30006032, period Q01 (quarter 1 of the quarterly fact).
# Population is annual, so it joins on the year; the left join keeps years without population (null).
q3 = (
    fact_quarterly
    .where((F.col('series_id') == 'PRS30006032') & (F.col('quarter') == 1))
    .select('series_id', F.lit('Q01').alias('period'), 'year', 'value')
    .join(us_population, 'year', 'left')
    .select('series_id', 'period', 'year', 'value', 'population')
    .orderBy('year')
)
display(q3)