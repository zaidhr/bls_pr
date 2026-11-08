from functools import reduce

from pyspark import pipelines as dp

from utilities.pr_files import (
    check_columns, dedupe, latest_version, layer_schema, not_null_expectations, schema_ddl, typed_select,
)

BRONZE = layer_schema(spark, 'bronze')
SILVER = layer_schema(spark, 'silver')

SOURCE_TABLES = [f'{BRONZE}.pr_data_0_current', f'{BRONZE}.pr_data_1_alldata']

SPEC = [
    ('series_id', 'STRING'),
    ('year', 'INT'),
    ('period', 'STRING'),
    ('value', 'DOUBLE'),
    ('footnote_codes', 'STRING'),
]
KEYS = ['series_id', 'year', 'period']


@dp.materialized_view(
    name=f'{SILVER}.pr_data',
    comment='Silver: BLS PR observations from pr.data.0.Current and pr.data.1.AllData, latest version, '
            'one row per series_id/year/period.',
    table_properties={'quality': 'silver'},
    cluster_by=['series_id', 'year'],
    schema=schema_ddl(SPEC, KEYS),
)
@dp.expect_all_or_fail(not_null_expectations(KEYS))
@dp.expect('value_is_numeric', 'value IS NOT NULL')
@dp.expect('valid_period', "period RLIKE '^Q0[1-5]$'")
def pr_data():
    frames = []
    for t in SOURCE_TABLES:
        df = spark.read.table(t)
        check_columns(df, [c for c, _ in SPEC], t)
        frames.append(typed_select(latest_version(df), SPEC))
    return dedupe(reduce(lambda a, b: a.unionByName(b), frames), KEYS)
