"""Unit tests for the Gold aggregations."""

from __future__ import annotations

from saas_pipeline import gold


def _fact(spark, rows):
    cols = [
        "_tenant_id",
        "fecha_proceso",
        "tipo_entrega",
        "ruta",
        "transporte",
        "material",
        "descripcion",
        "categoria",
        "cantidad_st",
        "precio",
    ]
    return spark.createDataFrame(rows, cols)


def test_daily_metrics_uses_normalized_units_and_transaction_price(spark):
    fact = _fact(
        spark,
        [
            ("sv", "20250314", "ZPRE", "1", "10", "M1", "d", "c", 60.0, "10.0"),
            ("sv", "20250314", "ZPRE", "2", "11", "M2", "d", "c", 5.0, "20.0"),
        ],
    )
    row = gold.compute_daily_metrics(fact).collect()[0]
    assert row["total_units"] == 65.0
    assert row["total_revenue"] == 60.0 * 10 + 5.0 * 20  # 700.0
    assert row["active_routes"] == 2
    assert row["active_transports"] == 2


def test_top_materials_ranks_by_revenue_and_limits_top_n(spark):
    fact = _fact(
        spark,
        [
            ("sv", "20250310", "ZPRE", "1", "1", "M1", "d1", "c", 10.0, "10.0"),  # rev 100
            ("sv", "20250320", "ZPRE", "2", "2", "M2", "d2", "c", 10.0, "50.0"),  # rev 500
            ("sv", "20250321", "ZPRE", "3", "3", "M3", "d3", "c", 1.0, "1.0"),  # rev 1
        ],
    )
    top = gold.compute_top_materials(fact, top_n=2).collect()
    assert len(top) == 2  # M3 dropped
    ranked = {r["rank"]: r["material"] for r in top}
    assert ranked[1] == "M2"
    assert ranked[2] == "M1"
    assert all(r["year_month"] == "202503" for r in top)
