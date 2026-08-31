"""validate_catalog must accept hyphenated UC names used by interop deploy."""

from __future__ import annotations

import pytest

from services.dbsql_service import validate_catalog


class TestValidateCatalog:
    def test_accepts_hyphenated_catalog_name(self) -> None:
        assert validate_catalog("taran-interop") is True

    @pytest.mark.parametrize(
        "catalog",
        [
            "classic_stable_realtime_demos_catalog",
            "iot_pharma",
            "Catalog_Name",
        ],
    )
    def test_accepts_simple_catalog_names(self, catalog: str) -> None:
        assert validate_catalog(catalog) is True

    @pytest.mark.parametrize(
        "catalog",
        [
            "",
            "foo'; DROP TABLE foo; --",
            "foo.bar",
            "foo bar",
            ";drop",
            "'quoted'",
            "1catalog",
        ],
    )
    def test_rejects_sql_injection_and_invalid_forms(self, catalog: str) -> None:
        assert validate_catalog(catalog) is False
