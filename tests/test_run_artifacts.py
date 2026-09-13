from pathlib import Path

from sregym.run_artifacts import RunArtifacts


def test_create_advances_attempt_when_resumed_run_directory_exists(tmp_path: Path) -> None:
    results = tmp_path / "results"
    existing = results / "agent" / "problem" / "run_1"
    existing.mkdir(parents=True)

    run = RunArtifacts.create(
        staging_root=tmp_path / ".runtime",
        results_root=results,
        problem_id="problem",
        agent="agent",
        attempt=1,
    )

    assert run.attempt == 2
    assert run.final_dir == results / "agent" / "problem" / "run_2"
