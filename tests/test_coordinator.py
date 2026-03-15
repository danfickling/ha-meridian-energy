"""Tests for coordinator.py — _build_period_meta."""

from __future__ import annotations

import pytest

from meridian_energy.coordinator import _build_period_meta



class TestBuildPeriodMeta:
    def test_structure(self):
        meta = _build_period_meta("Powershop")
        assert set(meta.keys()) == {"night", "peak", "offpeak", "weekend_offpeak", "controlled"}

    def test_names_include_sensor_name(self):
        meta = _build_period_meta("Powershop")
        assert meta["night"]["name"] == "Powershop (Night)"
        assert meta["peak"]["cost_name"] == "Powershop Cost (Peak)"

    def test_stat_ids_use_domain(self):
        meta = _build_period_meta("Powershop")
        assert meta["night"]["stat_id"] == "meridian_energy:consumption_night"
        assert meta["peak"]["cost_stat_id"] == "meridian_energy:cost_peak"

    def test_different_sensor_name(self):
        meta = _build_period_meta("Meridian")
        assert meta["offpeak"]["name"] == "Meridian (Off-Peak)"

    def test_all_periods_have_required_keys(self):
        meta = _build_period_meta("Test")
        for period, info in meta.items():
            assert "name" in info, f"{period} missing 'name'"
            assert "stat_id" in info, f"{period} missing 'stat_id'"
            assert "cost_name" in info, f"{period} missing 'cost_name'"
            assert "cost_stat_id" in info, f"{period} missing 'cost_stat_id'"
