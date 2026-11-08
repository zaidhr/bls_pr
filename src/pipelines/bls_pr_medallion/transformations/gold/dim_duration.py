from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_duration',
    comment='Duration dimension: % change same quarter 1 year ago, % change from previous quarter, '
            'Index (2017=100), Level. value_type = pct_change | index | level. Values of different '
            'value types / measures must never be summed together.',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('duration_code_not_null', 'duration_code IS NOT NULL')
@dp.expect('value_type_known', "value_type <> 'other'")
def dim_duration():
    d = spark.read.table(f'{SILVER}.pr_duration')
    text = F.lower(F.col('duration_text'))
    value_type = F.when(text.contains('%') | text.contains('change'), 'pct_change') \
        .when(text.startswith('index'), 'index') \
        .when(text.startswith('level'), 'level') \
        .otherwise('other')

    return d.select(
        'duration_code',
        'duration_text',
        value_type.alias('value_type'),
        'display_level',
        'selectable',
        'sort_sequence',
    )
