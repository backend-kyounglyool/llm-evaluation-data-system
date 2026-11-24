"""config 로더: 환경변수 치환과 SKILL_EVAL_OVERRIDES."""
from __future__ import annotations

import pytest

from common.config import apply_overrides, expand_env


def test_env_reference_uses_the_variable_then_the_default(monkeypatch):
    monkeypatch.setenv("RAW_URI", "s3://bucket/raw")
    monkeypatch.delenv("MISSING", raising=False)

    assert expand_env("raw: ${RAW_URI:-data/raw}") == "raw: s3://bucket/raw"
    assert expand_env("raw: ${MISSING:-data/raw}") == "raw: data/raw"
    assert expand_env("account: ${MISSING:-}") == "account: "
    assert expand_env("account: ${MISSING}") == "account: "


def test_overrides_replace_nested_values_with_yaml_types():
    config = {"llm": {"provider": "claude"}, "labeler": {"async": {"enabled": True, "workers": 4}}}

    apply_overrides(config, "llm.provider=mock, labeler.async.enabled=false,labeler.async.workers=1")

    assert config == {"llm": {"provider": "mock"}, "labeler": {"async": {"enabled": False, "workers": 1}}}


def test_override_of_unknown_key_is_rejected():
    with pytest.raises(KeyError, match="llm.provdier"):
        apply_overrides({"llm": {"provider": "claude"}}, "llm.provdier=mock")
