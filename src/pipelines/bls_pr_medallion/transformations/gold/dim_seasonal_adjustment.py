from pyspark import pipelines as dp

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_seasonal_adjustment',
    comment='Seasonal adjustment dimension (S = Seasonally Adjusted, U = Not Seasonally Adjusted).',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('seasonal_code_not_null', 'seasonal_code IS NOT NULL')
def dim_seasonal_adjustment():
    return spark.read.table(f'{SILVER}.pr_seasonal').select('seasonal_code', 'seasonal_text')
