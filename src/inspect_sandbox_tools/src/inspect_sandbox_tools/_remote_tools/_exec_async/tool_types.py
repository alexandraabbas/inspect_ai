from typing import Literal

from pydantic import BaseModel


class SubmitParams(BaseModel):
    """Parameters for exec_async_submit."""

    command: str
    model_config = {"extra": "forbid"}


class PollParams(BaseModel):
    """Parameters for exec_async_poll."""

    pid: int
    model_config = {"extra": "forbid"}


class KillParams(BaseModel):
    """Parameters for exec_async_kill."""

    pid: int
    model_config = {"extra": "forbid"}


class SubmitResult(BaseModel):
    """Result from exec_async_submit."""

    pid: int


class PollResult(BaseModel):
    """Result from exec_async_poll.

    Fields:
        state: Job lifecycle state - "running", "completed", or "killed"
        exit_code: Process exit code (only present when state is "completed")
        stdout: Standard output since last poll (incremental)
        stderr: Standard error since last poll (incremental)
    """

    state: Literal["running", "completed", "killed"]
    exit_code: int | None = None
    stdout: str
    stderr: str


class KillResult(BaseModel):
    """Result from exec_async_kill."""

    message: str
