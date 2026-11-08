import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import dedupe, json_record_expr, latest_version, layer_schema, not_null_expectations, schema_ddl

BRONZE = layer_schema(spark, 'bronze')
SILVER = layer_schema(spark, 'silver')

BRONZE_TABLE = f'{BRONZE}.acs_yg_total_population_1'

FIELDS = [
    ('Nation ID', 'nation_id', 'STRING'),
    ('Nation', 'nation', 'STRING'),
    ('Year', 'year', 'INT'),
    ('Population', 'population', 'BIGINT'),
]
SPEC = [(name, t) for _, name, t in FIELDS]
KEYS = ['nation_id', 'year']
EXPECTED_SOURCE_COLUMNS = json.dumps([src for src, _, _ in FIELDS], separators=(',', ':'))


@dp.materialized_view(
    name=f'{SILVER}.acs_yg_total_population_1',
    comment='Silver: US total population (Census ACS 1-year via DataUSA), latest version. One row per nation_id/year.',
    table_properties={'quality': 'silver', 'source_file_name': 'acs_yg_total_population_1.json'},
    schema=schema_ddl(SPEC, KEYS),
)
@dp.expect_all_or_fail(not_null_expectations(KEYS))
@dp.expect_all({'nation_not_null': 'nation IS NOT NULL', 'population_not_null': 'population IS NOT NULL'})
def acs_yg_total_population_1():
    records = latest_version(spark.read.table(BRONZE_TABLE)).select(
        F.expr('explode(try_cast(parse_json(raw_json):data AS ARRAY<VARIANT>))').alias('_rec'),
        '_source_file',
        '_source_file_modified_at',
    )
    typed = records.select(
        *[F.expr(json_record_expr(src, t)).alias(name) for src, name, t in FIELDS],
        '_source_file',
        '_source_file_modified_at',
    )
    return dedupe(typed, KEYS)


@dp.materialized_view(
    name='acs_yg_total_population_1_schema_check',
    private=True,
    comment="Fails the update if the DataUSA document's columns differ from the declared fields.",
)
@dp.expect_or_fail('source_columns_match', f"source_columns = '{EXPECTED_SOURCE_COLUMNS}'")
def acs_yg_total_population_1_schema_check():
    return latest_version(spark.read.table(BRONZE_TABLE)).select(
        F.expr('to_json(parse_json(raw_json):columns)').alias('source_columns'),
        '_source_file_modified_at',
    )
