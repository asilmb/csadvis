"""
Tests for:
  scraper/steam_market_scraper.py  — _resolve_container_type (helper)
  scraper/db_writer.py  — write_new_containers
  scraper/state.py      — needs_run, mark_done
  engine/portfolio_advisor.py — _earliest_date
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from scraper.db_writer import write_new_containers
from scraper.steam_market_scraper import ScrapedContainer, ScrapedItem, _resolve_container_type

# ─── _resolve_container_type ──────────────────────────────────────────────────


class TestResolveContainerType:
    def test_autograph_in_name_returns_autograph_capsule(self) -> None:
        assert (
            _resolve_container_type("Stockholm 2021 Autograph Capsule", "Sticker Capsule")
            == "Autograph Capsule"
        )

    def test_challengers_keyword_returns_event_capsule(self) -> None:
        assert (
            _resolve_container_type("Paris 2023 Challengers Sticker Capsule", "Sticker Capsule")
            == "Event Capsule"
        )

    def test_legends_keyword_returns_event_capsule(self) -> None:
        assert (
            _resolve_container_type("Antwerp 2022 Legends Sticker Capsule", "Sticker Capsule")
            == "Event Capsule"
        )

    def test_contenders_keyword_returns_event_capsule(self) -> None:
        assert (
            _resolve_container_type("Paris 2023 Contenders Sticker Capsule", "Sticker Capsule")
            == "Event Capsule"
        )

    def test_champions_keyword_returns_event_capsule(self) -> None:
        # "champions" without "autograph" in the name → Event Capsule
        assert (
            _resolve_container_type("Paris 2023 Champions Sticker Capsule", "Sticker Capsule")
            == "Event Capsule"
        )

    def test_plain_sticker_capsule_unchanged(self) -> None:
        assert (
            _resolve_container_type("Sticker Capsule Team Liquid", "Sticker Capsule")
            == "Sticker Capsule"
        )

    def test_weapon_case_raw_type_unchanged(self) -> None:
        assert _resolve_container_type("Revolution Case", "Weapon Case") == "Weapon Case"

    def test_souvenir_package_raw_type_unchanged(self) -> None:
        assert (
            _resolve_container_type("Paris 2023 Souvenir Package", "Souvenir Package")
            == "Souvenir Package"
        )

    def test_autograph_takes_priority_over_group_keyword(self) -> None:
        # "autograph" checked before group keywords
        result = _resolve_container_type(
            "Antwerp 2022 Challengers Autograph Capsule", "Sticker Capsule"
        )
        assert result == "Autograph Capsule"


# ─── write_new_containers ─────────────────────────────────────────────────────


def _make_scraped(name: str, ctype: str = "Weapon Case") -> ScrapedContainer:
    return ScrapedContainer(
        name=name,
        container_type=ctype,
        page_url=f"https://steamcommunity.com/market/listings/730/{name}",
        items=[ScrapedItem(base_name="AK-47 | Redline", rarity="Classified")],
    )


class TestWriteNewContainers:
    def _make_db(self, existing_names: list[str]) -> MagicMock:
        """Create a mock Session with existing container names."""
        db = MagicMock()
        existing = [MagicMock(container_name=n) for n in existing_names]
        db.query.return_value.all.return_value = existing
        return db

    def test_inserts_new_container(self) -> None:
        db = self._make_db([])
        containers = [_make_scraped("Revolution Case")]
        count = write_new_containers(db, containers)
        assert count == 1
        db.add.assert_called_once()
        db.commit.assert_called()

    def test_skips_existing_container(self) -> None:
        db = self._make_db(["Revolution Case"])
        containers = [_make_scraped("Revolution Case")]
        count = write_new_containers(db, containers)
        assert count == 0
        db.add.assert_not_called()

    def test_inserts_only_new_when_mixed(self) -> None:
        db = self._make_db(["Revolution Case"])
        containers = [
            _make_scraped("Revolution Case"),
            _make_scraped("Recoil Case"),
        ]
        count = write_new_containers(db, containers)
        assert count == 1

    def test_uses_default_cost_for_weapon_case(self) -> None:
        added_objects: list = []
        db = self._make_db([])
        db.add.side_effect = lambda obj: added_objects.append(obj)
        containers = [_make_scraped("Revolution Case", ctype="Weapon Case")]
        write_new_containers(db, containers)
        assert added_objects[0].base_cost == 1445  # Weapon Case default in KZT

    def test_uses_default_cost_for_sticker_capsule(self) -> None:
        added_objects: list = []
        db = self._make_db([])
        db.add.side_effect = lambda obj: added_objects.append(obj)
        # Must use a Steam-listed group capsule name (not an individual team capsule)
        containers = [
            _make_scraped("Paris 2023 Challengers Sticker Capsule", ctype="Sticker Capsule")
        ]
        write_new_containers(db, containers)
        assert added_objects[0].base_cost == 480  # Sticker Capsule default in KZT

    def test_skips_individual_team_capsule(self) -> None:
        """Individual team capsules are not on Steam Market — must be filtered out."""
        db = self._make_db([])
        containers = [
            _make_scraped("Copenhagen 2024 NaVi Autograph Capsule", ctype="Autograph Capsule")
        ]
        count = write_new_containers(db, containers)
        assert count == 0
        db.add.assert_not_called()

    def test_keeps_group_autograph_capsule(self) -> None:
        """Group capsules (Challengers/Legends/etc.) are listed on Steam Market."""
        db = self._make_db([])
        containers = [
            _make_scraped(
                "Copenhagen 2024 Challengers Autograph Capsule", ctype="Autograph Capsule"
            )
        ]
        count = write_new_containers(db, containers)
        assert count == 1

    def test_skips_unknown_container_type(self) -> None:
        db = self._make_db([])
        containers = [_make_scraped("Something", ctype="Unknown Type")]
        count = write_new_containers(db, containers)
        assert count == 0
        db.add.assert_not_called()

    def test_empty_input_returns_zero(self) -> None:
        db = self._make_db([])
        count = write_new_containers(db, [])
        assert count == 0


# ─── scraper/state.py ─────────────────────────────────────────────────────────


class TestScraperState:
    def _make_redis(self, stored_value: str | None) -> MagicMock:
        r = MagicMock()
        r.get.return_value = stored_value
        return r

    def test_needs_run_true_when_never_run(self) -> None:
        import scraper.state as st

        with patch.object(st, "get_redis", return_value=self._make_redis(None)):
            assert st.needs_run() is True

    def test_needs_run_false_when_already_today(self) -> None:
        from datetime import date

        import scraper.state as st

        today = date.today().isoformat()
        with patch.object(st, "get_redis", return_value=self._make_redis(today)):
            assert st.needs_run() is False

    def test_needs_run_true_when_yesterday(self) -> None:
        from datetime import date, timedelta

        import scraper.state as st

        yesterday = (date.today() - timedelta(days=1)).isoformat()
        with patch.object(st, "get_redis", return_value=self._make_redis(yesterday)):
            assert st.needs_run() is True

    def test_mark_done_saves_today(self) -> None:
        from datetime import date

        import scraper.state as st

        mock_redis = MagicMock()
        with patch.object(st, "get_redis", return_value=mock_redis):
            st.mark_done()
        today = date.today().isoformat()
        mock_redis.set.assert_called_once_with(st._KEY, today)


# ─── portfolio_advisor._earliest_date ─────────────────────────────────────────


class TestEarliestDate:
    def _h(self, ts: str) -> dict:
        return {"timestamp": ts, "price": 1.0}

    def test_returns_none_for_empty(self) -> None:
        from engine.portfolio_advisor import _earliest_date

        assert _earliest_date([]) is None

    def test_returns_single_date(self) -> None:
        from engine.portfolio_advisor import _earliest_date

        result = _earliest_date([self._h("2023-01-15 12:00")])
        assert result is not None
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15

    def test_returns_earliest_of_multiple(self) -> None:
        from engine.portfolio_advisor import _earliest_date

        history = [
            self._h("2023-06-01 00:00"),
            self._h("2022-01-01 00:00"),
            self._h("2024-12-31 00:00"),
        ]
        result = _earliest_date(history)
        assert result is not None
        assert result.year == 2022

    def test_skips_malformed_timestamps(self) -> None:
        from engine.portfolio_advisor import _earliest_date

        history = [
            {"timestamp": "not-a-date", "price": 1.0},
            self._h("2023-03-10 00:00"),
        ]
        result = _earliest_date(history)
        assert result is not None
        assert result.year == 2023

    def test_all_malformed_returns_none(self) -> None:
        from engine.portfolio_advisor import _earliest_date

        history = [{"timestamp": "bad", "price": 1.0}]
        assert _earliest_date(history) is None

    def test_supports_iso_format(self) -> None:
        from engine.portfolio_advisor import _earliest_date

        result = _earliest_date([{"timestamp": "2021-05-20T08:30:00", "price": 1.0}])
        assert result is not None
        assert result.year == 2021
