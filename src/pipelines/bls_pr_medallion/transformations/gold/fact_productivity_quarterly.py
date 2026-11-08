from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.fact_productivity_quarterly',
    comment='Quarterly BLS productivity & costs values. Grain: series_id x quarter (date_key = yyyyQ). '
            'Non-additive: always filter to one measure_code and duration_code.',
    table_properties={'quality': 'gold'},
    cluster_by=['series_id', 'date_key'],
)
@dp.expect_or_fail('series_in_dim_series', 'sector_code IS NOT NULL')
@dp.expect('value_not_null', 'value IS NOT NULL')
def fact_productivity_quarterly():
    data = spark.read.table(f'{SILVER}.pr_data').where(F.col('period').isin('Q01', 'Q02', 'Q03', 'Q04'))
    series = spark.read.table(f'{SILVER}.pr_series').select(
        'series_id',
        'sector_code',
        'measure_code',
        'duration_code',
        'class_code',
        F.col('seasonal').alias('seasonal_code'),
    )
    quarter = F.substring('period', 2, 2).cast('int')
    return data.join(series, 'series_id', 'left').select(
        'series_id',
        (F.col('year') * 10 + quarter).alias('date_key'),
        'year',
        quarter.alias('quarter'),
        'sector_code',
        'measure_code',
        'duration_code',
        'class_code',
        'seasonal_code',
        'value',
        F.coalesce(F.col('footnote_codes').contains('R'), F.lit(False)).alias('is_revised'),
        'footnote_codes',
    )
