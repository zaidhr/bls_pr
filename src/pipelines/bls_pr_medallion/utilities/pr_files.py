import os
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BRONZE_META_COLS = {'_rescued_data', '_source_file', '_source_file_modified_at', '_ingested_at'}

LINEAGE_COLS = ['_source_file', '_source_file_modified_at']

def layer_schema(spark, layer: str) -> str:
    return f'{spark.conf.get("pr.catalog")}.{spark.conf.get(f"pr.{layer}_schema")}'


# Tab Separated Reads

def to_identifier(name: str) -> str:
    return re.sub(r'[^0-9a-zA-Z]+', '_', name.strip()).strip('_').lower()


def is_tab_delimited(path: str) -> bool:
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        return '\t' in f.readline()


def discover_files(source_dir: str) -> list:
    files = []
    for name in sorted(os.listdir(source_dir)):
        path = os.path.join(source_dir, name)
        if name.startswith(('.', '_')) or not os.path.isfile(path):
            continue
        if is_tab_delimited(path):
            files.append(name)
    return files


# JSON Record Reads

def discover_json_files(source_dir: str) -> list:
    return [
        n for n in sorted(os.listdir(source_dir))
        if not n.startswith(('.', '_')) and n.lower().endswith('.json') and os.path.isfile(os.path.join(source_dir, n))
    ]


def json_table_name(file_name: str) -> str:
    return to_identifier(os.path.splitext(file_name)[0])


def json_record_expr(source_key: str, sql_type: str, variant_col: str = '_rec') -> str:
    field = f"{variant_col}:['{source_key}']"
    if sql_type in ('BIGINT', 'INT'):
        return f'try_cast(try_cast({field} AS DOUBLE) AS {sql_type})'
    return f'try_cast({field} AS {sql_type})'


# Silver Standardization

def latest_version(df: DataFrame) -> DataFrame:
    latest = df.groupBy('_source_file').agg(F.max('_source_file_modified_at').alias('_source_file_modified_at'))
    return df.join(latest, ['_source_file', '_source_file_modified_at'], 'inner')


def schema_ddl(spec: list, keys: list) -> str:
    cols = [f'{c} {t}{" NOT NULL" if c in keys else ""}' for c, t in spec]
    return ', '.join(cols + ['_source_file STRING', '_source_file_modified_at TIMESTAMP'])


def not_null_expectations(keys: list) -> dict:
    return {f'{k}_not_null': f'{k} IS NOT NULL' for k in keys}


def check_columns(df: DataFrame, expected: list, source: str) -> None:
    actual = [c for c in df.columns if c not in BRONZE_META_COLS]
    missing = [c for c in expected if c not in actual]
    unexpected = [c for c in actual if c not in expected]
    if missing or unexpected:
        raise ValueError(
            f'Schema drift in {source}: missing={missing}, unexpected={unexpected}. '
            'Update the silver definition to match the new source layout.'
        )


def cast_expr(col: str, sql_type: str) -> str:
    base = f"nullif(trim(`{col}`), '')"
    if sql_type == 'STRING':
        return base
    if sql_type == 'BOOLEAN':
        return f"CASE upper({base}) WHEN 'T' THEN true WHEN 'F' THEN false END"
    return f'try_cast({base} AS {sql_type})'


def typed_select(df: DataFrame, spec: list) -> DataFrame:
    cols = [c for c, _ in spec]
    out = df.select(*[F.expr(cast_expr(c, t)).alias(c) for c, t in spec], *LINEAGE_COLS)
    return out.where(F.coalesce(*[F.col(c).cast('string') for c in cols]).isNotNull())


def build_silver(df: DataFrame, spec: list, keys: list, source: str) -> DataFrame:
    check_columns(df, [c for c, _ in spec], source)
    return dedupe(typed_select(latest_version(df), spec), keys)


def dedupe(df: DataFrame, keys: list) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col('_source_file_modified_at').desc(), F.col('_source_file').desc())
    return df.withColumn('_rn', F.row_number().over(w)).where('_rn = 1').drop('_rn')
