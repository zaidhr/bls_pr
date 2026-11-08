from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_series',
    comment='Series dimension: title, base year and first/last period of each BLS PR series. '
            'The descriptive codes are also on the fact tables, so filter there; use this for labels/drill-through.',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('series_id_not_null', 'series_id IS NOT NULL')
def dim_series():
    return spark.read.table(f'{SILVER}.pr_series').select(
        'series_id',
        'series_title',
        'sector_code',
        'measure_code',
        'duration_code',
        'class_code',
        F.col('seasonal').alias('seasonal_code'),
        'base_year',
        'begin_year',
        'begin_period',
        'end_year',
        'end_period',
    )
