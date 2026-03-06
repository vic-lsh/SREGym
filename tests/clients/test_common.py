"""
Unit tests for the shared agent abstraction layer:
  - clients.common.base_agent          (BaseAgent template method + interceptor chain)
  - clients.common.summary_interceptor (SummaryInterceptor before/after)
  - clients.common.driver_utils        (build_instruction, save_results, wait_for_ready_stage, …)
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from clients.common.base_agent import (
    AfterRunContext,
    BaseAgent,
    BeforeRunContext,
    RunInterceptor,
)
from clients.common.driver_utils import (
    build_instruction,
    get_api_base_url,
    get_app_info,
    get_planned_stages,
    get_problem_id,
    save_results,
    wait_for_ready_stage,
)
from clients.common.summary_interceptor import SummaryInterceptor


# ---------------------------------------------------------------------------
# Stub concrete subclass used throughout this module
# ---------------------------------------------------------------------------


class _StubAgent(BaseAgent):
    """Minimal concrete subclass used to exercise BaseAgent behaviour."""

    _CLI_NAME = "stub-cli"
    _OUTPUT_FILENAME = "stub.txt"

    @classmethod
    def _install(cls) -> None:
        pass

    def _do_run(self, instruction: str, exp_env_dir: Path) -> int:
        self._last_instruction = instruction
        self._last_exp_env_dir = exp_env_dir
        return self._do_run_rc

    def get_usage_metrics(self) -> dict[str, int]:
        return {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}

    # controlled by tests
    _do_run_rc: int = 0
    _last_instruction: str = ""
    _last_exp_env_dir: Path = Path()


# ---------------------------------------------------------------------------
# BaseAgent: template method (run)
# ---------------------------------------------------------------------------


class TestBaseAgentRun:
    def _make_agent(self, tmp_path, interceptors=None, rc=0):
        agent = _StubAgent(logs_dir=tmp_path / "logs", model_name="m", interceptors=interceptors)
        agent._do_run_rc = rc
        return agent

    def test_run_returns_do_run_rc(self, tmp_path):
        agent = self._make_agent(tmp_path, rc=42)
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(tmp_path / "env")}):
            rc = agent.run("hello")
        assert rc == 42

    def test_run_writes_instruction_txt(self, tmp_path):
        agent = self._make_agent(tmp_path)
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(tmp_path / "env")}):
            agent.run("my instruction")
        assert (tmp_path / "logs" / "instruction.txt").read_text() == "my instruction"

    def test_run_creates_exp_env_dir(self, tmp_path):
        env_dir = tmp_path / "custom_env"
        agent = self._make_agent(tmp_path)
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(env_dir)}):
            agent.run("x")
        assert env_dir.is_dir()

    def test_run_passes_mutated_instruction_to_do_run(self, tmp_path):
        class MutatingInterceptor(RunInterceptor):
            def before_run(self, ctx: BeforeRunContext) -> None:
                ctx.instruction += " [mutated]"

        agent = self._make_agent(tmp_path, interceptors=[MutatingInterceptor()])
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(tmp_path / "env")}):
            agent.run("base")
        assert agent._last_instruction == "base [mutated]"

    def test_run_calls_before_interceptors_in_order(self, tmp_path):
        calls = []

        class A(RunInterceptor):
            def before_run(self, ctx):
                calls.append("A")

        class B(RunInterceptor):
            def before_run(self, ctx):
                calls.append("B")

        agent = self._make_agent(tmp_path, interceptors=[A(), B()])
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(tmp_path / "env")}):
            agent.run("x")
        assert calls == ["A", "B"]

    def test_run_calls_after_interceptors_in_reverse(self, tmp_path):
        calls = []

        class A(RunInterceptor):
            def after_run(self, ctx):
                calls.append("A")

        class B(RunInterceptor):
            def after_run(self, ctx):
                calls.append("B")

        agent = self._make_agent(tmp_path, interceptors=[A(), B()])
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(tmp_path / "env")}):
            agent.run("x")
        assert calls == ["B", "A"]

    def test_run_after_ctx_has_correct_return_code(self, tmp_path):
        received_rc = []

        class Recorder(RunInterceptor):
            def after_run(self, ctx: AfterRunContext):
                received_rc.append(ctx.return_code)

        agent = self._make_agent(tmp_path, interceptors=[Recorder()], rc=7)
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(tmp_path / "env")}):
            agent.run("x")
        assert received_rc == [7]

    def test_run_before_ctx_has_agent_and_exp_env_dir(self, tmp_path):
        received = {}

        class Recorder(RunInterceptor):
            def before_run(self, ctx: BeforeRunContext):
                received["agent"] = ctx.agent
                received["exp_env_dir"] = ctx.exp_env_dir

        agent = self._make_agent(tmp_path, interceptors=[Recorder()])
        env_dir = tmp_path / "env"
        with patch.dict(os.environ, {"SREGYM_EXP_ENV": str(env_dir)}):
            agent.run("x")
        assert received["agent"] is agent
        assert received["exp_env_dir"] == env_dir


# ---------------------------------------------------------------------------
# BaseAgent: installation helpers
# ---------------------------------------------------------------------------


class TestBaseAgentInstallation:
    def test_check_installation_true_when_found(self):
        with patch("shutil.which", return_value="/usr/bin/stub-cli"):
            assert _StubAgent.check_installation() is True

    def test_check_installation_false_when_not_found(self):
        with patch("shutil.which", return_value=None):
            assert _StubAgent.check_installation() is False

    def test_ensure_installed_no_op_when_already_present(self):
        with patch("shutil.which", return_value="/usr/bin/stub-cli"):
            _StubAgent.ensure_installed()  # should not raise

    def test_ensure_installed_raises_without_auto_install(self):
        with patch("shutil.which", return_value=None):
            with pytest.raises(RuntimeError, match="not installed"):
                _StubAgent.ensure_installed(auto_install=False)

    def test_ensure_installed_calls_install_then_verifies(self):
        with patch("shutil.which", side_effect=[None, "/usr/bin/stub-cli"]):
            with patch.object(_StubAgent, "_install") as mock_install:
                _StubAgent.ensure_installed(auto_install=True)
                mock_install.assert_called_once()

    def test_ensure_installed_raises_if_still_missing_after_install(self):
        with patch("shutil.which", return_value=None):
            with patch.object(_StubAgent, "_install"):
                with pytest.raises(RuntimeError, match="still not available"):
                    _StubAgent.ensure_installed(auto_install=True)


# ---------------------------------------------------------------------------
# BaseAgent: _run_subprocess
# ---------------------------------------------------------------------------


class TestRunSubprocess:
    def test_streams_output_to_file_and_stdout(self, tmp_path, capsys):
        agent = _StubAgent(logs_dir=tmp_path, model_name="m")
        fake_process = MagicMock()
        fake_process.stdout = iter(["line1\n", "line2\n"])
        fake_process.returncode = 0

        with patch("subprocess.Popen", return_value=fake_process):
            rc = agent._run_subprocess(["stub-cli"], tmp_path, {})

        assert rc == 0
        assert (tmp_path / "stub.txt").read_text() == "line1\nline2\n"
        out = capsys.readouterr().out
        assert "line1\n" in out
        assert "line2\n" in out

    def test_returns_process_returncode(self, tmp_path):
        agent = _StubAgent(logs_dir=tmp_path, model_name="m")
        fake_process = MagicMock()
        fake_process.stdout = iter([])
        fake_process.returncode = 5

        with patch("subprocess.Popen", return_value=fake_process):
            rc = agent._run_subprocess(["stub-cli"], tmp_path, {})

        assert rc == 5

    def test_passes_env_and_cwd_to_popen(self, tmp_path):
        agent = _StubAgent(logs_dir=tmp_path, model_name="m")
        fake_process = MagicMock()
        fake_process.stdout = iter([])
        fake_process.returncode = 0
        env = {"KEY": "VALUE"}

        with patch("subprocess.Popen", return_value=fake_process) as mock_popen:
            agent._run_subprocess(["stub-cli", "arg"], tmp_path / "cwd", env)

        _, kwargs = mock_popen.call_args
        assert kwargs["env"] == env
        assert kwargs["cwd"] == tmp_path / "cwd"


# ---------------------------------------------------------------------------
# SummaryInterceptor: before_run
# ---------------------------------------------------------------------------


class TestSummaryInterceptorBeforeRun:
    def _make_ctx(self, tmp_path, instruction="task"):
        agent = _StubAgent(logs_dir=tmp_path / "logs", model_name="m")
        exp_env_dir = tmp_path / "env"
        exp_env_dir.mkdir()
        return BeforeRunContext(instruction=instruction, agent=agent, exp_env_dir=exp_env_dir)

    def test_no_action_when_summary_file_absent(self, tmp_path):
        summary_dir = tmp_path / "summary"
        summary_dir.mkdir()
        ctx = self._make_ctx(tmp_path)
        original = ctx.instruction

        SummaryInterceptor(summary_dir=summary_dir, model_id="m", inject=True).before_run(ctx)

        assert ctx.instruction == original
        assert not (ctx.exp_env_dir / "long_term_summary.txt").exists()

    def test_no_action_when_inject_false(self, tmp_path):
        summary_dir = tmp_path / "summary"
        summary_dir.mkdir()
        (summary_dir / "long_term_summary.txt").write_text("past findings")
        ctx = self._make_ctx(tmp_path)
        original = ctx.instruction

        SummaryInterceptor(summary_dir=summary_dir, model_id="m", inject=False).before_run(ctx)

        assert ctx.instruction == original
        assert not (ctx.exp_env_dir / "long_term_summary.txt").exists()

    def test_copies_summary_and_appends_note_when_inject_true(self, tmp_path):
        summary_dir = tmp_path / "summary"
        summary_dir.mkdir()
        (summary_dir / "long_term_summary.txt").write_text("past findings")
        ctx = self._make_ctx(tmp_path, instruction="do the thing")

        SummaryInterceptor(summary_dir=summary_dir, model_id="m", inject=True).before_run(ctx)

        assert (ctx.exp_env_dir / "long_term_summary.txt").read_text() == "past findings"
        assert "long_term_summary.txt" in ctx.instruction
        assert ctx.instruction.startswith("do the thing")

    def test_summary_path_property(self, tmp_path):
        summary_dir = tmp_path / "summary"
        interceptor = SummaryInterceptor(summary_dir=summary_dir, model_id="m")
        assert interceptor.summary_path == summary_dir / "long_term_summary.txt"


# ---------------------------------------------------------------------------
# SummaryInterceptor: after_run
# ---------------------------------------------------------------------------


class TestSummaryInterceptorAfterRun:
    def test_after_run_calls_result_summarizer(self, tmp_path):
        agent = _StubAgent(logs_dir=tmp_path / "logs", model_name="m")
        summary_dir = tmp_path / "summary"
        interceptor = SummaryInterceptor(summary_dir=summary_dir, model_id="my-model", inject=False)
        ctx = AfterRunContext(return_code=0, agent=agent)

        with patch("clients.common.summary_interceptor.ResultSummarizer") as MockRS:
            mock_instance = MagicMock()
            MockRS.return_value = mock_instance
            interceptor.after_run(ctx)

        MockRS.assert_called_once_with(
            logs_dir=agent.logs_dir,
            model_id="my-model",
            output_filename=agent._OUTPUT_FILENAME,
            summary_dir=summary_dir,
        )
        mock_instance.run.assert_called_once()


# ---------------------------------------------------------------------------
# driver_utils: get_api_base_url
# ---------------------------------------------------------------------------


class TestGetApiBaseUrl:
    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("API_HOSTNAME", None)
            os.environ.pop("API_PORT", None)
            url = get_api_base_url()
        assert url == "http://localhost:8000"

    def test_uses_env_vars(self):
        with patch.dict(os.environ, {"API_HOSTNAME": "myhost", "API_PORT": "9999"}):
            assert get_api_base_url() == "http://myhost:9999"


# ---------------------------------------------------------------------------
# driver_utils: build_instruction
# ---------------------------------------------------------------------------


class TestBuildInstruction:
    _APP = {"app_name": "myapp", "namespace": "prod", "descriptions": "An app."}

    def test_no_planned_stages_includes_mitigation(self):
        instr = build_instruction(self._APP, "p1", planned_stages=None)
        assert "TASK 2: MITIGATION" in instr
        assert "TWO tasks" in instr

    def test_diagnosis_only_excludes_mitigation(self):
        instr = build_instruction(self._APP, "p1", planned_stages=["diagnosis"])
        assert "TASK 2: MITIGATION" not in instr
        assert "ONE task" in instr

    def test_diagnosis_and_mitigation_includes_mitigation(self):
        instr = build_instruction(self._APP, "p1", planned_stages=["diagnosis", "mitigation"])
        assert "TASK 2: MITIGATION" in instr
        assert "TWO tasks" in instr

    def test_extra_preamble_appears_in_output(self):
        instr = build_instruction(self._APP, "p1", extra_preamble="CRITICAL: automated mode")
        assert "CRITICAL: automated mode" in instr

    def test_app_fields_appear(self):
        instr = build_instruction(self._APP, "p1")
        assert "myapp" in instr
        assert "prod" in instr
        assert "An app." in instr

    def test_submit_url_appears(self):
        with patch.dict(os.environ, {"API_HOSTNAME": "conductor", "API_PORT": "8080"}):
            instr = build_instruction(self._APP, "p1")
        assert "http://conductor:8080/submit" in instr


# ---------------------------------------------------------------------------
# driver_utils: save_results
# ---------------------------------------------------------------------------


class TestSaveResults:
    def test_creates_json_file_with_prefix(self, tmp_path):
        save_results(tmp_path, "prob-42", 0, {"input_tokens": 10}, prefix="gemini_cli")
        files = list(tmp_path.glob("gemini_cli_results_prob-42_*.json"))
        assert len(files) == 1
        data = json.loads(files[0].read_text())
        assert data["problem_id"] == "prob-42"
        assert data["return_code"] == 0
        assert data["success"] is True
        assert data["usage_metrics"] == {"input_tokens": 10}

    def test_failure_sets_success_false(self, tmp_path):
        save_results(tmp_path, "p", 1, {}, prefix="codex")
        files = list(tmp_path.glob("codex_results_p_*.json"))
        data = json.loads(files[0].read_text())
        assert data["success"] is False

    def test_prefix_used_in_filename(self, tmp_path):
        save_results(tmp_path, "p", 0, {}, prefix="claudecode")
        assert len(list(tmp_path.glob("claudecode_results_*.json"))) == 1


# ---------------------------------------------------------------------------
# driver_utils: wait_for_ready_stage
# ---------------------------------------------------------------------------


class TestWaitForReadyStage:
    def _mock_response(self, stage):
        resp = MagicMock()
        resp.json.return_value = {"stage": stage}
        return resp

    def test_returns_immediately_on_diagnosis(self):
        with patch("clients.common.driver_utils.requests.get",
                   return_value=self._mock_response("diagnosis")):
            assert wait_for_ready_stage() == "diagnosis"

    def test_returns_immediately_on_mitigation(self):
        with patch("clients.common.driver_utils.requests.get",
                   return_value=self._mock_response("mitigation")):
            assert wait_for_ready_stage() == "mitigation"

    def test_polls_until_ready(self):
        responses = [
            self._mock_response("setup"),
            self._mock_response("setup"),
            self._mock_response("diagnosis"),
        ]
        with patch("clients.common.driver_utils.requests.get", side_effect=responses):
            with patch("clients.common.driver_utils.time.sleep"):
                assert wait_for_ready_stage() == "diagnosis"

    def test_raises_timeout(self):
        with patch("clients.common.driver_utils.requests.get",
                   return_value=self._mock_response("setup")):
            with patch("clients.common.driver_utils.time.sleep"):
                with patch("clients.common.driver_utils.time.time", side_effect=[0, 0, 400]):
                    with pytest.raises(TimeoutError):
                        wait_for_ready_stage(timeout=300)

    def test_retries_on_exception(self):
        with patch("clients.common.driver_utils.requests.get",
                   side_effect=[Exception("conn refused"), self._mock_response("diagnosis")]):
            with patch("clients.common.driver_utils.time.sleep"):
                assert wait_for_ready_stage() == "diagnosis"


# ---------------------------------------------------------------------------
# driver_utils: conductor API helpers
# ---------------------------------------------------------------------------


class TestConductorApiHelpers:
    def test_get_app_info(self):
        resp = MagicMock()
        resp.json.return_value = {"app_name": "foo"}
        with patch("clients.common.driver_utils.requests.get", return_value=resp):
            assert get_app_info() == {"app_name": "foo"}

    def test_get_problem_id(self):
        resp = MagicMock()
        resp.json.return_value = {"problem_id": "diag-001"}
        with patch("clients.common.driver_utils.requests.get", return_value=resp):
            assert get_problem_id() == "diag-001"

    def test_get_planned_stages(self):
        resp = MagicMock()
        resp.json.return_value = {"stages": ["diagnosis", "mitigation"]}
        with patch("clients.common.driver_utils.requests.get", return_value=resp):
            assert get_planned_stages() == ["diagnosis", "mitigation"]
