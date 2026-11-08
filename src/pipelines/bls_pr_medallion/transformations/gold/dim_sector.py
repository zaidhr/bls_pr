from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_sector',
    comment='Sector dimension (e.g. Nonfarm Business, Manufacturing). parent_sector_code links sub-sectors '
            '(Durable / Nondurable Manufacturing) to their parent.',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('sector_code_not_null', 'sector_code IS NOT NULL')
def dim_sector():
    s = spark.read.table(f'{SILVER}.pr_sector').select(
        'sector_code',
        'sector_name',
        'display_level',
        'selectable',
        'sort_sequence'
    )
    p = s.select(
        F.col('sector_code').alias('parent_sector_code'),
        F.col('display_level').alias('_p_level'),
        F.col('sort_sequence').alias('_p_sort'),
    )
    candidates = s.join(
        p, (F.col('_p_level') == F.col('display_level') - 1) & (F.col('_p_sort') < F.col('sort_sequence')), 'left'
    )
    w = Window.partitionBy('sector_code').orderBy(F.col('_p_sort').desc_nulls_last())
    return candidates.withColumn('_rn', F.row_number().over(w)) \
        .where('_rn = 1') \
        .select(
            'sector_code',
            'sector_name',
            'parent_sector_code',
            'display_level',
            'selectable',
            'sort_sequence'
        )
