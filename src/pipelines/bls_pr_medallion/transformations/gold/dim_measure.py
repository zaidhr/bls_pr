from pyspark import pipelines as dp

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_measure',
    comment='Measure dimension (e.g. Labor productivity, Unit labor costs, Employment).',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('measure_code_not_null', 'measure_code IS NOT NULL')
def dim_measure():
    return spark.read.table(f'{SILVER}.pr_measure').select(
        'measure_code',
        'measure_text',
        'display_level',
        'selectable',
        'sort_sequence'
    )
