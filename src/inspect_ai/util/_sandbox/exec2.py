"""Exec2 - Asynchronous command execution with streaming output.

This module provides the host-side implementation for exec2, enabling
long-running commands in sandbox environments with streaming output.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar

from pydantic import BaseModel

from inspect_ai.tool._json_rpc_helpers import exec_model_request
from inspect_ai.tool._sandbox_tools_utils._runtime_helpers import (
    SandboxJSONRPCTransport,
    SandboxToolsServerErrorMapper,
)
from inspect_ai.tool._sandbox_tools_utils.sandbox import SANDBOX_TOOLS_CLI

if TYPE_CHECKING:
    from .._subprocess import ExecResult
    from .environment import SandboxEnvironment


# ============================================================================
# Event Types
# ============================================================================


@dataclass
class StdoutChunk:
    """A chunk of stdout data from the running process."""

    data: str


@dataclass
class StderrChunk:
    """A chunk of stderr data from the running process."""

    data: str


@dataclass
class Completed:
    """Process completed (successfully or with error).

    Attributes:
        exit_code: The process exit code (0 = success).
    """

    exit_code: int

    @property
    def success(self) -> bool:
        """Return True if the process exited successfully (exit code 0)."""
        return self.exit_code == 0


Exec2Event = StdoutChunk | StderrChunk | Completed
"""Union type for all events that can be yielded by Exec2Process.events."""


# ============================================================================
# Options
# ============================================================================


@dataclass
class Exec2Options:
    """Options for exec2() command execution.

    Attributes:
        cwd: Working directory for command execution.
        env: Additional environment variables.
        user: User to run the command as.
        poll_interval: Interval between poll requests (defaults to 0.5 seconds).
    """

    cwd: str | None = None
    env: dict[str, str] | None = None
    user: str | None = None
    poll_interval: float | None = None


# ============================================================================
# JSON-RPC Response Types (mirrors server-side types)
# ============================================================================


class _SubmitResult(BaseModel):
    """Result from exec_async_submit."""

    pid: int


class _PollResult(BaseModel):
    """Result from exec_async_poll."""

    state: Literal["running", "completed", "killed"]
    exit_code: int | None = None
    stdout: str
    stderr: str


class _KillResult(BaseModel):
    """Result from exec_async_kill."""

    message: str


# ============================================================================
# Constants
# ============================================================================

DEFAULT_POLL_INTERVAL = 0.5
"""Default interval between poll requests in seconds."""

RPC_TIMEOUT = 30
"""Timeout for individual JSON-RPC calls in seconds."""

T = TypeVar("T", bound=BaseModel)


class Exec2Process:
    """Handle to a running exec2 process.

    This class is an async iterator that yields events as they arrive.
    It can only be iterated once (single-use iterator pattern).

    Usage patterns:

    1. Streaming: iterate over the process directly
       ```python
       proc = await sandbox.exec2(["cmd"])
       async for event in proc:
           match event:
               case StdoutChunk(data=data): print(data)
               case Completed(exit_code=code): print(f"Done: {code}")
       ```

    2. Fire-and-forget with explicit kill:
       ```python
       proxy = await sandbox.exec2(["./proxy"])
       # ... do other work ...
       await proxy.kill()  # terminate when done
       ```
    """

    def __init__(
        self,
        sandbox: SandboxEnvironment,
        cmd: list[str],
        options: Exec2Options,
    ) -> None:
        """Initialize an Exec2Process.

        Args:
            sandbox: The sandbox environment where the process will run.
            cmd: Command and arguments to execute.
            options: Execution options.
        """
        self._sandbox = sandbox
        self._cmd = cmd
        self._options = options
        self._poll_interval = options.poll_interval or DEFAULT_POLL_INTERVAL
        self._pid: int | None = None
        self._killed = False
        self._completed = False
        self._iteration_started = False
        self._pending_events: list[Exec2Event] = []

    @property
    def pid(self) -> int:
        """Return the process ID."""
        if self._pid is None:
            raise RuntimeError("Process has not been submitted yet")
        return self._pid

    # -------------------------------------------------------------------------
    # RPC helpers
    # -------------------------------------------------------------------------

    async def _rpc(
        self, method: str, params: dict[str, object], result_type: type[T]
    ) -> T:
        """Make an RPC call to the sandbox."""
        transport = SandboxJSONRPCTransport(self._sandbox, SANDBOX_TOOLS_CLI)
        server_error_mapper = SandboxToolsServerErrorMapper()
        return await exec_model_request(
            method=method,
            params=params,
            result_type=result_type,
            transport=transport,
            server_error_mapper=server_error_mapper,
            timeout=RPC_TIMEOUT,
            user=self._options.user,
        )

    def _build_shell_command(self) -> str:
        """Build a shell command string from command list and options.

        Returns a shell command with environment variables and working directory
        applied. The order ensures env vars apply to the command, not to `cd`:
            cd /path && VAR=value command args
        """
        shell_cmd = shlex.join(self._cmd)

        # Add environment variables if specified (applies to command, not cd)
        if self._options.env:
            env_prefix = " ".join(
                f"{k}={shlex.quote(v)}" for k, v in self._options.env.items()
            )
            shell_cmd = f"{env_prefix} {shell_cmd}"

        # Add working directory if specified (wraps the env+command)
        if self._options.cwd:
            shell_cmd = f"cd {shlex.quote(self._options.cwd)} && {shell_cmd}"

        return shell_cmd

    async def _submit(self) -> None:
        """Submit the job to the sandbox."""
        shell_cmd = self._build_shell_command()
        result = await self._rpc(
            "exec_async_submit", {"command": shell_cmd}, _SubmitResult
        )
        self._pid = result.pid

    # -------------------------------------------------------------------------
    # Async Iterator Protocol
    # -------------------------------------------------------------------------

    def __aiter__(self) -> "Exec2Process":
        """Return self as the async iterator.

        This class implements the async iterator protocol directly.
        It can only be iterated once - subsequent iterations will raise RuntimeError.
        """
        if self._iteration_started:
            raise RuntimeError("Exec2Process can only be iterated once")
        self._iteration_started = True
        return self

    async def __anext__(self) -> Exec2Event:
        """Return the next event from the process.

        Yields StdoutChunk and StderrChunk events as output becomes available,
        then yields a final Completed event when the process terminates.

        Note: After the Completed event is yielded, the job is automatically
        cleaned up on the server side.

        If cancelled, the process will be killed before re-raising the exception.

        Raises:
            StopAsyncIteration: When the process has completed or been killed.
            RuntimeError: If the process has not been submitted yet.
        """
        import asyncio

        import anyio

        if self._pid is None:
            raise RuntimeError("Process has not been submitted yet")

        # Return any pending events first
        if self._pending_events:
            return self._pending_events.pop(0)

        # If already in terminal state, stop iteration
        if self._completed or self._killed:
            raise StopAsyncIteration

        try:
            while True:
                result = await self._rpc(
                    "exec_async_poll", {"pid": self._pid}, _PollResult
                )

                # Collect events from this poll
                events: list[Exec2Event] = []
                if result.stdout:
                    events.append(StdoutChunk(data=result.stdout))
                if result.stderr:
                    events.append(StderrChunk(data=result.stderr))

                # Check for terminal state
                if result.state == "completed":
                    self._completed = True
                    if result.exit_code is None:
                        raise RuntimeError(
                            "Server returned completed state without exit_code"
                        )
                    events.append(Completed(exit_code=result.exit_code))
                elif result.state == "killed":
                    # Process was killed (possibly by another call to kill())
                    self._killed = True
                    # Don't yield Completed for killed processes - kill() discards output

                # If we have events, return the first and queue the rest
                if events:
                    self._pending_events = events[1:]
                    return events[0]

                # If killed with no events, stop iteration
                if self._killed:
                    raise StopAsyncIteration

                # Still running with no output, wait before polling again
                await asyncio.sleep(self._poll_interval)

        except anyio.get_cancelled_exc_class():
            # Kill the process on cancellation to avoid leaving orphaned processes
            await self.kill()
            raise

    async def kill(self) -> None:
        """Terminate the process.

        Calling kill() indicates the caller is uninterested in the process output
        or exit code. Any buffered data is discarded.

        If the process has already completed or been killed, this is a no-op.
        """
        if self._pid is None or self._completed or self._killed:
            return

        self._killed = True
        await self._rpc("exec_async_kill", {"pid": self._pid}, _KillResult)


# ============================================================================
# Factory Functions
# ============================================================================


async def exec2_streaming(
    sandbox: SandboxEnvironment,
    cmd: list[str],
    options: Exec2Options | None = None,
) -> Exec2Process:
    """Create and start an exec2 process for streaming.

    Args:
        sandbox: The sandbox environment to run the command in.
        cmd: Command and arguments to execute.
        options: Execution options.

    Returns:
        Exec2Process handle that can be iterated for events, or killed.
    """
    proc = Exec2Process(sandbox, cmd, options or Exec2Options())
    await proc._submit()
    return proc


async def exec2_awaitable(
    sandbox: SandboxEnvironment,
    cmd: list[str],
    options: Exec2Options | None = None,
) -> ExecResult[str]:
    """Run a command and return the result without streaming.

    Submits the command, polls until completion, and returns ExecResult.
    If cancelled, the process will be killed before re-raising the exception.

    Args:
        sandbox: The sandbox environment to run the command in.
        cmd: Command and arguments to execute.
        options: Execution options.

    Returns:
        ExecResult[str] with success, returncode, stdout, and stderr.
    """
    from .._subprocess import ExecResult as ExecResultClass

    proc = await exec2_streaming(sandbox, cmd, options)

    # Accumulate output chunks
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    async for event in proc:
        if isinstance(event, StdoutChunk):
            stdout_chunks.append(event.data)
        elif isinstance(event, StderrChunk):
            stderr_chunks.append(event.data)
        elif isinstance(event, Completed):
            return ExecResultClass[str](
                success=event.success,
                returncode=event.exit_code,
                stdout="".join(stdout_chunks),
                stderr="".join(stderr_chunks),
            )

    # If we get here, the process was killed (no Completed event)
    return ExecResultClass[str](
        success=False,
        returncode=-1,
        stdout="".join(stdout_chunks),
        stderr="".join(stderr_chunks),
    )
