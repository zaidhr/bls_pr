from pyspark import pipelines as dp

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_worker_class',
    comment='Worker class dimension: which workers a series covers (Employees, All workers).',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('class_code_not_null', 'class_code IS NOT NULL')
def dim_worker_class():
    return spark.read.table(f'{SILVER}.pr_class').select(
        'class_code',
        'class_text',
        'display_level',
        'selectable',
        'sort_sequence'
    )
