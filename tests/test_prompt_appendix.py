"""Tests for the opt-in agent prompt appendix."""

from __future__ import annotations

from clients.codex.driver import build_instruction
from clients.harness.prompt_appendix import PROMPT_APPENDIX_ENV, append_prompt_appendix
from sregym.service.container_runner import ContainerRunner

APP_INFO = {"app_name": "Hotel Reservation", "namespace": "hotel-reservation", "descriptions": "A hotel app."}


def test_unset_or_blank_appendix_leaves_instruction_unchanged(monkeypatch):
    monkeypatch.delenv(PROMPT_APPENDIX_ENV, raising=False)
    assert append_prompt_appendix("diagnose") == "diagnose"
    monkeypatch.setenv(PROMPT_APPENDIX_ENV, "  \n ")
    assert append_prompt_appendix("diagnose") == "diagnose"


def test_appendix_is_appended_after_the_instruction(monkeypatch):
    monkeypatch.setenv(PROMPT_APPENDIX_ENV, "\nVERIFY:\n1. Reproduce.\n")

    assert append_prompt_appendix("diagnose") == "diagnose\n\nVERIFY:\n1. Reproduce."


def test_codex_instruction_is_unchanged_without_the_appendix(monkeypatch):
    monkeypatch.delenv(PROMPT_APPENDIX_ENV, raising=False)
    monkeypatch.delenv("SREGYM_SUMMARY_FILE", raising=False)
    plain = build_instruction(APP_INFO)

    monkeypatch.setenv(PROMPT_APPENDIX_ENV, "VERIFY: reproduce the symptom first.")
    extended = build_instruction(APP_INFO)

    assert "VERIFY" not in plain
    assert extended == f"{plain}\n\nVERIFY: reproduce the symptom first."


def test_codex_instruction_places_the_appendix_before_operational_memory(tmp_path, monkeypatch):
    memory = tmp_path / "long_term_summary.md"
    memory.write_text("Earlier runs saw a missing ConfigMap.", encoding="utf-8")
    monkeypatch.setenv("SREGYM_SUMMARY_FILE", str(memory))
    monkeypatch.setenv(PROMPT_APPENDIX_ENV, "VERIFY: reproduce the symptom first.")

    instruction = build_instruction(APP_INFO)

    assert instruction.index("VERIFY") < instruction.index("OPERATIONAL MEMORY")


def test_container_runner_forwards_the_appendix(monkeypatch):
    monkeypatch.setenv(PROMPT_APPENDIX_ENV, "VERIFY:\nline two")

    flags = ContainerRunner()._build_env_flags()

    assert f"{PROMPT_APPENDIX_ENV}=VERIFY:\nline two" in flags
