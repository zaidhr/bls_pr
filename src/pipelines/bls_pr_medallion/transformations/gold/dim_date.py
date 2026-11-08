from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_date',
    comment='Conformed date dimension. date_key = yyyyQ for quarters (Q1-Q4) and yyyy0 for years '
            '(annual values, BLS period Q05). is_annual distinguishes the two grains.',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('date_key_not_null', 'date_key IS NOT NULL')
def dim_date():
    years = spark.read.table(f'{SILVER}.pr_data').select('year') \
        .unionByName(spark.read.table(f'{SILVER}.acs_yg_total_population_1').select('year')) \
        .agg(F.min('year').alias('min_year'), F.max('year').alias('max_year')) \
        .select(F.explode(F.sequence('min_year', 'max_year')).alias('year')) \

    quarters = years.crossJoin(spark.range(1, 5).select(F.col('id').cast('int').alias('quarter'))).select(
        (F.col('year') * 10 + F.col('quarter')).alias('date_key'),
        'year',
        'quarter',
        F.concat(F.lit('Q'), F.lpad(F.col('quarter').cast('string'), 2, '0')).alias('period_code'),
        F.lit(False).alias('is_annual'),
        F.make_date('year', (F.col('quarter') - 1) * 3 + 1, F.lit(1)).alias('start_date'),
        F.last_day(F.make_date('year', F.col('quarter') * 3, F.lit(1))).alias('end_date'),
    )

    annual = years.select(
        (F.col('year') * 10).alias('date_key'),
        'year',
        F.lit(None).cast('int').alias('quarter'),
        F.lit('Q05').alias('period_code'),
        F.lit(True).alias('is_annual'),
        F.make_date('year', F.lit(1), F.lit(1)).alias('start_date'),
        F.make_date('year', F.lit(12), F.lit(31)).alias('end_date'),
    )

    periods = spark.read.table(f'{SILVER}.pr_period').select(
        F.col('period').alias('period_code'), 'period_abbr', 'period_name'
    )

    return quarters.unionByName(annual) \
        .join(periods, 'period_code', 'left') \
        .select(
            'date_key',
            'year',
            'quarter',
            'period_code',
            'period_abbr',
            'period_name',
            'is_annual',
            'start_date',
            'end_date'
        )
