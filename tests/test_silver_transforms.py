"""Unit tests for the Silver transformation logic."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F

from saas_pipeline import transforms


def _deliveries(spark, rows):
    cols = [
        "pais",
        "fecha_proceso",
        "transporte",
        "ruta",
        "tipo_entrega",
        "material",
        "precio",
        "cantidad",
        "unidad",
    ]
    return spark.createDataFrame(rows, cols)


def test_normalize_units_converts_cases_to_units(spark):
    df = _deliveries(
        spark,
        [
            ("SV", "20250314", "1", "1", "ZPRE", "AA004003", "10.0", "3", "CS"),
            ("SV", "20250314", "2", "2", "ZPRE", "AA004003", "10.0", "3", "ST"),
        ],
    )
    out = transforms.normalize_units(df, units_per_case=20).collect()
    by_transport = {r["transporte"]: r["cantidad_st"] for r in out}
    assert by_transport["1"] == 60.0  # 3 cases * 20
    assert by_transport["2"] == 3.0  # already in ST


def test_split_by_delivery_type_discards_out_of_scope(spark):
    df = _deliveries(
        spark,
        [
            ("SV", "20250314", "1", "1", "ZPRE", "M", "1", "1", "ST"),
            ("SV", "20250314", "2", "2", "Z05", "M", "1", "1", "ST"),
            ("SV", "20250314", "3", "3", "COBR", "M", "1", "1", "ST"),
            ("SV", "20250314", "4", "4", "Z99", "M", "1", "1", "ST"),
        ],
    )
    kept, discarded = transforms.split_by_delivery_type(df, ["ZPRE", "ZVE1", "Z04", "Z05"])
    assert kept.count() == 2
    assert {r["tipo_entrega"] for r in discarded.collect()} == {"COBR", "Z99"}


def test_quarantine_reason_flags_value_anomalies(spark):
    df = _deliveries(
        spark,
        [
            ("SV", "00000000", "1", "1", "ZPRE", "M", "10", "5", "ST"),  # bad date
            ("SV", "20250314", "2", "2", "ZPRE", "M", "10", "-1", "ST"),  # bad qty
            ("SV", "20250314", "3", "3", "ZPRE", "M", None, "5", "ST"),  # null price
            ("SV", "20250314", "4", "4", "ZPRE", "M", "10", "5", "ST"),  # clean
        ],
    )
    out = {
        r["transporte"]: r["_quarantine_reason"]
        for r in transforms.with_quarantine_reason(df).collect()
    }
    assert out["1"] == "invalid_fecha_proceso"
    assert out["2"] == "invalid_cantidad"
    assert out["3"] == "null_precio"
    assert out["4"] is None


def test_material_integrity_flags_unknown_sku(spark):
    df = transforms.with_quarantine_reason(
        _deliveries(
            spark,
            [
                ("SV", "20250314", "1", "1", "ZPRE", "AA004003", "10", "5", "ST"),
                ("SV", "20250314", "2", "2", "ZPRE", "XX999999", "10", "5", "ST"),
            ],
        )
    )
    catalog = spark.createDataFrame([("AA004003",)], ["material"])
    out = {
        r["transporte"]: r["_quarantine_reason"]
        for r in transforms.flag_material_integrity(df, catalog).collect()
    }
    assert out["1"] is None
    assert out["2"] == "material_not_in_catalog"


def test_temporal_join_selects_version_in_force_not_current(spark):
    # AA004003 changed price on 2025-04-01. A 2025-03 transaction must resolve to
    # the OLD version (31.95), not the current one (33.80).
    fact = _deliveries(
        spark, [("SV", "20250315", "1", "1", "ZPRE", "AA004003", "50", "5", "ST")]
    ).withColumn("fecha_date", F.to_date(F.lit("2025-03-15")))

    dim_cols = [
        "material",
        "descripcion",
        "categoria",
        "precio_base",
        "valid_from",
        "valid_to",
        "is_current",
    ]
    dim = spark.createDataFrame(
        [
            ("AA004003", "Cola", "BEB", 31.95, dt.date(2024, 1, 1), dt.date(2025, 3, 31), False),
            ("AA004003", "Cola", "BEB", 33.80, dt.date(2025, 4, 1), dt.date(9999, 12, 31), True),
        ],
        dim_cols,
    )
    row = transforms.enrich_with_materials(fact, dim).collect()[0]
    assert float(row["precio_base"]) == 31.95
    assert row["categoria"] == "BEB"


def test_deduplicate_exact_removes_full_duplicates(spark):
    rows = [("SV", "20250314", "1", "1", "ZPRE", "M", "10", "5", "ST")] * 3
    assert transforms.deduplicate_exact(_deliveries(spark, rows)).count() == 1
