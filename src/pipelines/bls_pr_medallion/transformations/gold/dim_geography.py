from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import layer_schema

SILVER = layer_schema(spark, 'silver')
GOLD = layer_schema(spark, 'gold')


@dp.materialized_view(
    name=f'{GOLD}.dim_geography',
    comment='Geography dimension for the population star. geography_level allows states/counties to be added later.',
    table_properties={'quality': 'gold'},
)
@dp.expect_or_fail('geography_id_not_null', 'geography_id IS NOT NULL')
def dim_geography():
    return spark.read.table(f'{SILVER}.acs_yg_total_population_1') \
        .select(
            F.col('nation_id').alias('geography_id'),
            F.col('nation').alias('geography_name'),
            F.lit('nation').alias('geography_level'),
        ) \
        .dropDuplicates(['geography_id'])
