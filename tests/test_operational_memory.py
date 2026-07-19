"""Tests for cross-run operational-memory injection."""

from __future__ import annotations

from clients.harness.memory import inject_operational_memory


def test_missing_memory_configuration_leaves_instruction_unchanged(monkeypatch):
    monkeypatch.delenv("SREGYM_SUMMARY_FILE", raising=False)
    assert inject_operational_memory("diagnose") == "diagnose"


def test_memory_is_injected_with_verification_caveat(tmp_path, monkeypatch):
    memory = tmp_path / "long_term_summary.md"
    memory.write_text("Checkout failures previously came from PRODUCT_CATALOG_ADDR.", encoding="utf-8")
    monkeypatch.setenv("SREGYM_SUMMARY_FILE", str(memory))

    instruction = inject_operational_memory("Diagnose the current incident.")

    assert instruction.startswith("Diagnose the current incident.")
    assert "PRODUCT_CATALOG_ADDR" in instruction
    assert "verify it against the current cluster" in instruction
