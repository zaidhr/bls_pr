from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.pr_files import discover_files, discover_json_files, json_table_name, to_identifier

SOURCE_DIR = spark.conf.get('pr.source_dir')


def define_bronze_table(file_name: str):
    table_name = to_identifier(file_name)

    @dp.table(
        name=table_name,
        comment=f"Bronze: raw rows from BLS PR file '{file_name}' (all columns STRING, as published).",
        table_properties={'quality': 'bronze', 'source_file_name': file_name},
    )
    def bronze():
        df = spark.readStream.format('cloudFiles') \
            .option('cloudFiles.format', 'csv') \
            .option('cloudFiles.allowOverwrites', 'true') \
            .option('sep', '\t') \
            .option('header', 'true') \
            .option('pathGlobFilter', file_name) \
            .load(SOURCE_DIR)
        renamed = [
            F.col(f'`{c}`').alias(c if c == '_rescued_data' else to_identifier(c))
            for c in df.columns
        ]
        return df.select(
            *renamed,
            F.col('_metadata.file_path').alias('_source_file'),
            F.col('_metadata.file_modification_time').alias('_source_file_modified_at'),
            F.current_timestamp().alias('_ingested_at'),
        )


def define_bronze_json_table(file_name: str):
    @dp.table(
        name=json_table_name(file_name),
        comment=f"Bronze: raw JSON document from '{file_name}' (one row per published version, unparsed).",
        table_properties={'quality': 'bronze', 'source_file_name': file_name},
    )
    def bronze_json():
        df = spark.readStream.format('cloudFiles') \
            .option('cloudFiles.format', 'text') \
            .option('wholetext', 'true') \
            .option('cloudFiles.allowOverwrites', 'true') \
            .option('pathGlobFilter', file_name) \
            .load(SOURCE_DIR)

        return df.select(
            F.col('value').alias('raw_json'),
            F.col('_metadata.file_path').alias('_source_file'),
            F.col('_metadata.file_modification_time').alias('_source_file_modified_at'),
            F.current_timestamp().alias('_ingested_at'),
        )


for file in discover_files(SOURCE_DIR):
    define_bronze_table(file)

for file in discover_json_files(SOURCE_DIR):
    define_bronze_json_table(file)
