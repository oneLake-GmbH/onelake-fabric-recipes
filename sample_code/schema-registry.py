"""
schema_registry.py
------------------
Git-versioned schema validation for Microsoft Fabric Lakehouses.

Pattern:
  1. Bootstrap  – first run infers schema and writes <table>_InitialLoad.json
  2. Review     – engineer reviews, renames file to <table>.json, commits to Git
  3. Sync       – Git syncs to Azure Storage Account → shortcutted into Lakehouse
  4. Validate   – every ETL run loads the approved JSON and validates incoming data
"""

import json
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCHEMA_REGISTRY_ROOT = "/lakehouse/default/Files/integration-schema-registry/Tables"


# ---------------------------------------------------------------------------
# 1. Bootstrap: infer schema from DataFrame and write <table>_InitialLoad.json
# ---------------------------------------------------------------------------

def bootstrap_schema(df: DataFrame, schema_name: str, table_name: str) -> str:
    """
    Infer schema from a DataFrame and write a bootstrap JSON file.
    The engineer must review this file, rename it to <table>.json, and commit to Git.
    Raises ValueError to halt the pipeline until the schema is approved.
    """
    schema_dir = f"{SCHEMA_REGISTRY_ROOT}/{schema_name}"
    bootstrap_path = f"{schema_dir}/{table_name}_InitialLoad.json"

    os.makedirs(schema_dir, exist_ok=True)

    schema_json = json.loads(df.schema.json())
    with open(bootstrap_path, "w", encoding="utf-8") as f:
        json.dump(schema_json, f, indent=2, ensure_ascii=False)

    raise ValueError(
        f"Schema bootstrap created at: {bootstrap_path}\n"
        f"  → Review the file\n"
        f"  → Rename it to {table_name}.json\n"
        f"  → Commit to Git and re-run the pipeline"
    )


# ---------------------------------------------------------------------------
# 2. Load: read approved schema JSON from Lakehouse (synced from Storage Account)
# ---------------------------------------------------------------------------

def load_approved_schema(schema_name: str, table_name: str) -> StructType:
    """
    Load the approved schema JSON from the Lakehouse.
    If the approved file is missing but a bootstrap exists, guide the engineer.
    If neither exists, trigger bootstrap.
    """
    schema_dir = f"{SCHEMA_REGISTRY_ROOT}/{schema_name}"
    approved_path = f"{schema_dir}/{table_name}.json"
    bootstrap_path = f"{schema_dir}/{table_name}_InitialLoad.json"

    if os.path.exists(approved_path):
        with open(approved_path, "r", encoding="utf-8") as f:
            schema_json = json.load(f)
        return StructType.fromJson(schema_json)

    if os.path.exists(bootstrap_path):
        raise ValueError(
            f"Bootstrap schema found at {bootstrap_path} but no approved schema exists.\n"
            f"  → Review, rename to {table_name}.json, commit to Git, and re-run."
        )

    raise FileNotFoundError(
        f"No schema file found for {schema_name}/{table_name}. "
        f"Run bootstrap_schema() to create one."
    )


# ---------------------------------------------------------------------------
# 3. Validate: compare incoming DataFrame schema against the approved schema
# ---------------------------------------------------------------------------

def validate_schema(df: DataFrame, approved_schema: StructType) -> None:
    """
    Compare the incoming DataFrame schema against the approved (Git-versioned) schema.
    Raises ValueError with a full drift report if mismatches are found.

    Checks:
      - Missing columns  (in approved schema but absent from incoming data)
      - Unexpected columns (present in incoming data but not in approved schema)
      - Type mismatches  (column exists in both but data types differ)
    """
    actual    = {f.name: str(f.dataType) for f in df.schema.fields}
    expected  = {f.name: str(f.dataType) for f in approved_schema.fields}

    missing     = set(expected) - set(actual)
    unexpected  = set(actual)   - set(expected)
    type_drifts = {
        col: {"expected": expected[col], "actual": actual[col]}
        for col in expected.keys() & actual.keys()
        if expected[col] != actual[col]
    }

    errors = []
    if missing:
        errors.append(f"  Missing columns    : {sorted(missing)}")
    if unexpected:
        errors.append(f"  Unexpected columns : {sorted(unexpected)}")
    if type_drifts:
        errors.append(f"  Type mismatches    : {type_drifts}")

    if errors:
        raise ValueError("Schema drift detected:\n" + "\n".join(errors))

    print("Schema validation passed.")


# ---------------------------------------------------------------------------
# 4. ETL entry point — wire everything together
# ---------------------------------------------------------------------------

def run_etl(spark: SparkSession, source_path: str, schema_name: str, table_name: str):
    """
    Full ETL flow with schema validation gate.

    First run  → bootstraps schema file, halts pipeline.
    Subsequent → loads approved schema, validates incoming data, continues.
    """
    df_incoming = spark.read.option("header", True).csv(source_path)

    # -- Schema gate ----------------------------------------------------------
    try:
        approved_schema = load_approved_schema(schema_name, table_name)
    except FileNotFoundError:
        # First ever run: infer & write bootstrap, then abort
        bootstrap_schema(df_incoming, schema_name, table_name)

    # Validate incoming data against approved (Git-versioned) schema
    validate_schema(df_incoming, approved_schema)

    # -- Cast to approved schema (safe, type-aligned write) -------------------
    df_typed = spark.createDataFrame(df_incoming.rdd, schema=approved_schema)

    # -- Write to Delta Lakehouse table ---------------------------------------
    target_path = f"/lakehouse/default/Tables/{schema_name}/{table_name}"
    df_typed.write.format("delta").mode("append").save(target_path)

    print(f"ETL complete. Written to {target_path}")


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SchemaRegistry").getOrCreate()

    run_etl(
        spark=spark,
        source_path="/lakehouse/default/Files/raw/sales/orders.csv",
        schema_name="sales",
        table_name="orders",
    )
