"""Task 1: SQL-safe quoting for hyphenated Unity Catalog identifiers."""

from __future__ import annotations

from services.uc_ident import sql_fqn, sql_ident


class TestSqlIdent:
    def test_hyphenated_catalog_is_backtick_quoted(self) -> None:
        assert sql_ident("taran-interop") == "`taran-interop`"

    def test_simple_identifier_stays_unquoted(self) -> None:
        assert sql_ident("rtm_lakets_zb") == "rtm_lakets_zb"


class TestSqlFqn:
    def test_hyphenated_catalog_part_is_quoted(self) -> None:
        assert (
            sql_fqn("taran-interop", "rtm_lakets_zb", "pharmaceutical_microbatch")
            == "`taran-interop`.rtm_lakets_zb.pharmaceutical_microbatch"
        )
