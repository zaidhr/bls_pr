from pyspark import pipelines as dp

from utilities.pr_files import build_silver, layer_schema, not_null_expectations, schema_ddl

BRONZE = layer_schema(spark, 'bronze')
SILVER = layer_schema(spark, 'silver')

_HIERARCHY = [('display_level', 'INT'), ('selectable', 'BOOLEAN'), ('sort_sequence', 'INT')]

REFERENCE_TABLES = {
    'pr_class': ('pr.class', [('class_code', 'STRING'), ('class_text', 'STRING'), *_HIERARCHY], ['class_code']),
    'pr_duration': ('pr.duration', [('duration_code', 'STRING'), ('duration_text', 'STRING'), *_HIERARCHY], ['duration_code']),
    'pr_footnote': ('pr.footnote', [('footnote_code', 'STRING'), ('footnote_text', 'STRING')], ['footnote_code']),
    'pr_measure': ('pr.measure', [('measure_code', 'STRING'), ('measure_text', 'STRING'), *_HIERARCHY], ['measure_code']),
    'pr_period': ('pr.period', [('period', 'STRING'), ('period_abbr', 'STRING'), ('period_name', 'STRING')], ['period']),
    'pr_seasonal': ('pr.seasonal', [('seasonal_code', 'STRING'), ('seasonal_text', 'STRING')], ['seasonal_code']),
    'pr_sector': ('pr.sector', [('sector_code', 'STRING'), ('sector_name', 'STRING'), *_HIERARCHY], ['sector_code']),
    'pr_series': (
        'pr.series',
        [
            ('series_id', 'STRING'),
            ('sector_code', 'STRING'),
            ('class_code', 'STRING'),
            ('measure_code', 'STRING'),
            ('duration_code', 'STRING'),
            ('seasonal', 'STRING'),
            ('base_year', 'INT'),
            ('footnote_codes', 'STRING'),
            ('series_title', 'STRING'),
            ('begin_year', 'INT'),
            ('begin_period', 'STRING'),
            ('end_year', 'INT'),
            ('end_period', 'STRING'),
        ],
        ['series_id', 'sector_code', 'class_code', 'measure_code', 'duration_code', 'seasonal'],
    ),
}


def define(table: str, source_file: str, spec: list, not_null: list):
    key = not_null[0]
    bronze_table = f'{BRONZE}.{table}'

    @dp.materialized_view(
        name=f'{SILVER}.{table}',
        comment=f"Silver: cleaned, typed, latest version of BLS PR file '{source_file}'. One row per {key}.",
        table_properties={'quality': 'silver', 'source_file_name': source_file},
        schema=schema_ddl(spec, not_null),
    )
    @dp.expect_all_or_fail(not_null_expectations(not_null))
    def silver():
        return build_silver(spark.read.table(bronze_table), spec, [key], bronze_table)


for table, (file, spec, not_null) in REFERENCE_TABLES.items():
    define(table, file, spec, not_null)
