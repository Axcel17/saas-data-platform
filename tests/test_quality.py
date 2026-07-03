"""Unit tests for the data-quality framework."""

from __future__ import annotations

from saas_pipeline import quality


def _fact(spark, rows):
    cols = [
        "_tenant_id",
        "fecha_proceso",
        "ruta",
        "transporte",
        "tipo_entrega",
        "cantidad_st",
        "categoria",
    ]
    return spark.createDataFrame(rows, cols)


def _dim(spark, rows):
    cols = ["material", "is_current"]
    return spark.createDataFrame(rows, cols)


def test_checks_pass_on_clean_data(spark):
    fact = _fact(
        spark,
        [
            ("sv", "20250314", "1", "1", "ZPRE", 60.0, "BEBIDAS"),
            ("sv", "20250314", "2", "2", "Z04", 5.0, "AGUA"),
        ],
    )
    dim = _dim(spark, [("AA004003", True), ("AA004003", False)])
    results = quality.run_silver_checks(fact, dim)

    assert all(r.passed for r in results)
    assert not quality.has_critical_failure(results)


def test_critical_failure_detected(spark):
    # cantidad_st <= 0 and an out-of-scope delivery type both violate critical checks.
    fact = _fact(
        spark,
        [
            ("sv", "20250314", "1", "1", "ZPRE", 0.0, "BEBIDAS"),
            ("sv", "20250314", "2", "2", "COBR", 5.0, "AGUA"),
        ],
    )
    dim = _dim(spark, [("AA004003", True)])
    results = quality.run_silver_checks(fact, dim)

    assert quality.has_critical_failure(results)
    by_name = {r.check_name: r for r in results}
    assert by_name["cantidad_st_positive"].records_failed == 1
    assert by_name["delivery_type_in_scope"].records_failed == 1


def test_duplicate_current_version_is_critical(spark):
    fact = _fact(spark, [("sv", "20250314", "1", "1", "ZPRE", 5.0, "AGUA")])
    dim = _dim(spark, [("AA004003", True), ("AA004003", True)])  # two current versions
    results = quality.run_silver_checks(fact, dim)

    by_name = {r.check_name: r for r in results}
    assert by_name["single_current_version_per_material"].records_failed == 1
    assert quality.has_critical_failure(results)


def test_check_result_passed_property():
    ok = quality.CheckResult("c", quality.INFO, "silver", "t", 10, 0)
    bad = quality.CheckResult("c", quality.CRITICAL, "silver", "t", 10, 2)
    assert ok.passed is True
    assert bad.passed is False
