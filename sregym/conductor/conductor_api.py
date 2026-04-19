import asyncio
import logging
import os
import threading
import time

import pyfiglet
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from uvicorn import Config, Server

from sregym.conductor.constants import MAX_DIAGNOSIS_CANDIDATES

app = FastAPI()
_conductor = None

_server: Server | None = None
_shutdown_event = threading.Event()

logger = logging.getLogger("all.sregym.conductor_api")


def request_shutdown():
    """
    Signal the API server to shut down.
    Safe to call from any thread and idempotent.
    """
    logger.warning("Shutting down API server...")
    _shutdown_event.set()
    if _server is not None:
        _server.should_exit = True


def set_conductor(c):
    """Inject the shared Conductor instance."""
    global _conductor
    _conductor = c


class SubmitRequest(BaseModel):
    # A diagnosis submission may be a single string answer or a list of
    # candidate diagnoses (up to MAX_DIAGNOSIS_CANDIDATES). The latter is
    # graded by checking whether the ground truth matches *any* candidate.
    solution: str | list[str]


@app.post("/submit")
async def submit_solution(req: SubmitRequest):
    allowed = {"diagnosis", "mitigation"}
    if _conductor is None or _conductor.submission_stage not in allowed:
        logger.error(f"Cannot submit at stage: {_conductor.submission_stage!r}")
        raise HTTPException(status_code=400, detail=f"Cannot submit at stage: {_conductor.submission_stage!r}")

    if isinstance(req.solution, list):
        if len(req.solution) == 0:
            raise HTTPException(status_code=400, detail="Submission list must not be empty.")
        if len(req.solution) > MAX_DIAGNOSIS_CANDIDATES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Submission list has {len(req.solution)} candidates; "
                    f"maximum allowed is {MAX_DIAGNOSIS_CANDIDATES}."
                ),
            )

    # Use repr() to properly escape special characters in the solution; this
    # produces a valid Python literal for either str or list[str], which the
    # parser then decodes via ast.parse.
    wrapped = f"```\nsubmit({repr(req.solution)})\n```"
    logger.debug(f"Wrapped submit content: {wrapped}")

    try:
        results = await _conductor.submit(wrapped)
    except Exception as e:
        logger.error(f"Grading error: {e}")
        raise HTTPException(status_code=400, detail=f"Grading error: {e}")

    logger.debug(f"API returns Grading results by now: {results}")
    return results


class SubmitStageRequest(BaseModel):
    # Autonomous-submit variant: the agent names the stage explicitly
    # ("diagnosis" or "mitigation") so submissions can be graded in any
    # order (or skipped) without touching the sequential state machine.
    solution: str | list[str]
    stage: str


@app.post("/submit_stage")
async def submit_stage(req: SubmitStageRequest):
    """Grade a single stage in autonomous-submit mode.

    The response is deliberately neutral (``{"status": "recorded"}``): the
    whole point of autonomous mode is that the agent must self-verify via
    the cluster, so leaking the oracle verdict here would defeat the
    design. Results are still stored on the conductor for offline scoring.
    """
    if _conductor is None:
        logger.error("No conductor set; cannot submit stage.")
        raise HTTPException(status_code=400, detail="No problem has been started")

    _ALLOWED_STAGES = {"diagnosis", "mitigation"}
    if req.stage not in _ALLOWED_STAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown stage {req.stage!r}; allowed: {sorted(_ALLOWED_STAGES)}",
        )

    if isinstance(req.solution, list):
        if len(req.solution) == 0:
            raise HTTPException(status_code=400, detail="Submission list must not be empty.")
        if len(req.solution) > MAX_DIAGNOSIS_CANDIDATES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Submission list has {len(req.solution)} candidates; "
                    f"maximum allowed is {MAX_DIAGNOSIS_CANDIDATES}."
                ),
            )

    try:
        await _conductor.submit_autonomous(req.stage, req.solution)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Autonomous grading error: {e}")
        raise HTTPException(status_code=500, detail=f"Autonomous grading error: {e}") from e

    return {"status": "recorded"}


@app.post("/cleanup")
async def post_cleanup():
    """Trigger deferred teardown. Only valid when submission_stage is
    "awaiting_cleanup". Returns a noop response if teardown already ran."""
    if _conductor is None:
        logger.error("No problem has been started")
        raise HTTPException(status_code=400, detail="No problem has been started")

    stage = _conductor.submission_stage
    if stage == "done":
        return {"status": "noop", "stage": stage}
    if stage != "awaiting_cleanup":
        raise HTTPException(
            status_code=409,
            detail=f"/cleanup is only valid when stage is 'awaiting_cleanup' (current: {stage!r})",
        )

    await asyncio.to_thread(_conductor.force_cleanup)
    return {"status": "ok", "stage": _conductor.submission_stage}


@app.get("/status")
async def get_status():
    if _conductor is None:
        logger.error("No problem has been started")
        raise HTTPException(status_code=400, detail="No problem has been started")
    stage = _conductor.submission_stage
    logger.debug(f"API returns Current stage: {stage}")
    return {"stage": stage}


@app.get("/get_app")
async def get_app():
    if _conductor is None:
        logger.error("No problem has been started")
        raise HTTPException(status_code=400, detail="No problem has been started")
    app_inst = _conductor.app
    logger.debug(f"API returns App instance: {app_inst}")
    return {"app_name": app_inst.app_name, "namespace": app_inst.namespace, "descriptions": str(app_inst.description)}


@app.get("/get_problem")
async def get_problem():
    if _conductor is None:
        logger.error("No problem has been started")
        raise HTTPException(status_code=400, detail="No problem has been started")
    problem_id = _conductor.problem_id
    logger.debug(f"API returns Problem ID: {problem_id}")
    return {"problem_id": problem_id}


@app.get("/stages")
async def get_stages():
    if _conductor is None:
        logger.error("No problem has been started")
        raise HTTPException(status_code=400, detail="No problem has been started")
    stage_names = [s["name"] for s in _conductor.stage_sequence]
    logger.debug(f"API returns planned stages: {stage_names}")
    return {"stages": stage_names}


def run_api(conductor):
    """
    Start the API server and block until request_shutdown() is called.
    """
    global _server
    set_conductor(conductor)
    logger.debug(f"API server is binded to the conductor {conductor}")

    # Load from .env with defaults
    host = os.getenv("API_HOSTNAME", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))

    logger.debug(f"API server starting on http://{host}:{port}")

    console = Console()
    art = pyfiglet.figlet_format("SREGym")
    console.print(Panel(art, title="SREGym API Server", subtitle=f"http://{host}:{port}", style="bold green"))
    console.print(
        Markdown(
            """
**Available Endpoints**
- **POST /submit**: `{ "solution": "<your-solution>" }` → grades the current stage
- **POST /cleanup**: triggers deferred teardown (only valid when stage is `awaiting_cleanup`)
- **GET /status**: returns `{ "stage": "setup" | "diagnosis" | "mitigation" | "awaiting_cleanup" | "done" }`
- **GET /stages**: returns `{ "stages": ["diagnosis", "mitigation", ...] }` — planned stage sequence
"""
        )
    )

    config = Config(app=app, host=host, port=port, log_level="info")
    config.install_signal_handlers = False
    server = Server(config)
    _server = server  # expose to request_shutdown()

    # watcher thread: when _shutdown_event is set, flip server.should_exit
    def _watch():
        _shutdown_event.wait()
        logger.debug("API server shutdown event received")
        server.should_exit = True
        
        # Keep ensuring should_exit is True until the server actually stops
        # (check global _server which is cleared in finally block)
        while _server is not None:
            server.should_exit = True
            time.sleep(0.1)

    threading.Thread(target=_watch, name="api-shutdown-watcher", daemon=True).start()

    try:
        logger.debug("API server is running")
        server.run()  # blocks until should_exit becomes True
    finally:
        # cleanup for potential reuse
        _shutdown_event.clear()
        _server = None
