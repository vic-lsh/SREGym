from langgraph.graph import add_messages
from typing_extensions import Annotated, TypedDict


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    num_steps: int
    submitted: bool
    rollback_stack: str
    submission_retries: int


class JudgeState(TypedDict):
    messages: Annotated[list, add_messages]
    num_steps: int
    submitted: bool
    verdict: str  # "APPROVED" | "REJECTED" | ""
    rollback_stack: str
    submission_retries: int
