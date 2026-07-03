"""Explicit schemas for source files and control tables.

Reading the raw CSVs with a fixed schema (instead of ``inferSchema``) keeps
Bronze deterministic across runs and environments, and preserves the original
values as strings so anomaly detection happens in Silver rather than being
masked by silent cast failures at read time.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Deliveries are read fully as strings. Types listed as "long"/"decimal" in the
# spec are parsed explicitly in Silver, where invalid values are quarantined
# instead of becoming nulls at read time.
DELIVERIES_RAW_SCHEMA = StructType(
    [
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("transporte", StringType(), True),
        StructField("ruta", StringType(), True),
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", StringType(), True),
        StructField("cantidad", StringType(), True),
        StructField("unidad", StringType(), True),
    ]
)

# The catalog already ships with valid_from/valid_to/is_current, so it is a
# well-formed SCD Type 2 source. Types are trustworthy here.
MATERIALS_RAW_SCHEMA = StructType(
    [
        StructField("material", StringType(), True),
        StructField("descripcion", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("precio_base", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", StringType(), True),
    ]
)

# Shared quality-logs table (architecture section 5.9).
QUALITY_LOGS_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)
