from __future__ import annotations

import pytest

from fintxn.config import _deep_merge, _interpolate, load_settings


def test_ci_profile_uses_local_backend_and_small_data() -> None:
    s = load_settings("ci")
    assert s.env == "ci"
    assert s.storage.backend == "local"
    assert s.data.n_transactions == 10_000
    assert s.spark.shuffle_partitions == 4


def test_local_profile_targets_s3_api() -> None:
    s = load_settings("local")
    assert s.storage.backend == "s3"
    assert s.storage.bucket("gold") == "fintxn-gold"


def test_env_interpolation_with_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO_X", "bar")
    monkeypatch.delenv("MISSING_X", raising=False)
    assert _interpolate("a-${FOO_X}") == "a-bar"
    assert _interpolate("${MISSING_X:-dflt}") == "dflt"
    assert _interpolate("${MISSING_X}") is None
    assert _interpolate({"k": ["${FOO_X}"]}) == {"k": ["bar"]}


def test_deep_merge_overrides_nested_keys_only() -> None:
    merged = _deep_merge({"a": {"b": 1, "c": 2}, "d": 3}, {"a": {"c": 9}})
    assert merged == {"a": {"b": 1, "c": 9}, "d": 3}


def test_overrides_argument() -> None:
    s = load_settings("ci", overrides={"data": {"n_transactions": 123}})
    assert s.data.n_transactions == 123
