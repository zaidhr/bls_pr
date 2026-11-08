from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.fact_population_annual',
    comment='Annual total population (Census ACS 1-year estimate via DataUSA). Grain: geography x year '
            '(date_key = yyyy0, same year rows as fact_productivity_annual).',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('keys_not_null', 'geography_id IS NOT NULL AND date_key IS NOT NULL')
@dp.expect('population_positive', 'population > 0')
def fact_population_annual():
    return spark.read.table(f'{SILVER}.acs_yg_total_population_1').select(
        F.col('nation_id').alias('geography_id'),
        (F.col('year') * 10).alias('date_key'),
        'year',
        'population',
    )
