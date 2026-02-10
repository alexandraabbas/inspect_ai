"""Exec2 - Asynchronous command execution with streaming output.

This module provides the host-side implementation for exec2, enabling
long-running commands in sandbox environments with streaming output.
"""

from __future__ import annotations

import shlex
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

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
        stdout: Full accumulated stdout from the process.
        stderr: Full accumulated stderr from the process.
    """

    exit_code: int
    stdout: str
    stderr: str

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


# ============================================================================
# Exec2Process
# ============================================================================


class Exec2Process:
    """Handle to a running exec2 process.

    Usage patterns:

    1. Streaming: iterate over events
       ```python
       proc = await sandbox.exec2(["cmd"])
       async for event in proc.events:
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
        pid: int,
        poll_interval: float,
        user: str | None,
    ) -> None:
        """Initialize an Exec2Process handle.

        This constructor is internal. Use sandbox.exec2() to create instances.

        Args:
            sandbox: The sandbox environment where the process is running.
            pid: The process ID returned from exec_async_submit.
            poll_interval: Interval between poll requests.
            user: User to run commands as (for RPC calls).
        """
        self._sandbox = sandbox
        self._pid = pid
        self._poll_interval = poll_interval
        self._user = user
        self._killed = False
        self._completed = False

    @property
    def pid(self) -> int:
        """Return the process ID."""
        return self._pid

    @property
    async def events(self) -> AsyncIterator[Exec2Event]:
        """Async iterator over events as they arrive.

        Yields StdoutChunk and StderrChunk events as output becomes available,
        then yields a final Completed event when the process terminates.

        Note: After the Completed event is yielded, the job is automatically
        cleaned up on the server side. Subsequent calls will raise an error.

        If cancelled, the process will be killed before re-raising the exception.
        """
        import asyncio

        import anyio

        # Accumulate full output for the Completed event
        full_stdout: list[str] = []
        full_stderr: list[str] = []

        transport = SandboxJSONRPCTransport(self._sandbox, SANDBOX_TOOLS_CLI)
        server_error_mapper = SandboxToolsServerErrorMapper()

        try:
            while not self._completed and not self._killed:
                result = await exec_model_request(
                    method="exec_async_poll",
                    params={"pid": self._pid},
                    result_type=_PollResult,
                    transport=transport,
                    server_error_mapper=server_error_mapper,
                    timeout=RPC_TIMEOUT,
                    user=self._user,
                )

                # Yield stdout chunks
                if result.stdout:
                    full_stdout.append(result.stdout)
                    yield StdoutChunk(data=result.stdout)

                # Yield stderr chunks
                if result.stderr:
                    full_stderr.append(result.stderr)
                    yield StderrChunk(data=result.stderr)

                # Check for terminal state
                if result.state == "completed":
                    self._completed = True
                    assert result.exit_code is not None
                    yield Completed(
                        exit_code=result.exit_code,
                        stdout="".join(full_stdout),
                        stderr="".join(full_stderr),
                    )
                elif result.state == "killed":
                    # Process was killed (possibly by another call to kill())
                    self._killed = True
                    # Don't yield Completed for killed processes - kill() discards output
                else:
                    # Still running, wait before polling again
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
        if self._completed or self._killed:
            return

        self._killed = True

        transport = SandboxJSONRPCTransport(self._sandbox, SANDBOX_TOOLS_CLI)
        server_error_mapper = SandboxToolsServerErrorMapper()

        await exec_model_request(
            method="exec_async_kill",
            params={"pid": self._pid},
            result_type=_KillResult,
            transport=transport,
            server_error_mapper=server_error_mapper,
            timeout=RPC_TIMEOUT,
            user=self._user,
        )


# ============================================================================
# Helper Functions
# ============================================================================


def _build_shell_command(cmd: list[str], options: Exec2Options) -> str:
    """Build a shell command string from command list and options.

    Args:
        cmd: Command and arguments to execute.
        options: Execution options.

    Returns:
        Shell command string with environment variables and working directory.
    """
    # exec_async uses create_subprocess_shell, so we need to pass a shell command string
    shell_cmd = shlex.join(cmd)

    # Add working directory if specified
    if options.cwd:
        shell_cmd = f"cd {shlex.quote(options.cwd)} && {shell_cmd}"

    # Add environment variables if specified
    if options.env:
        env_prefix = " ".join(f"{k}={shlex.quote(v)}" for k, v in options.env.items())
        shell_cmd = f"{env_prefix} {shell_cmd}"

    return shell_cmd


async def _submit_job(
    sandbox: SandboxEnvironment,
    cmd: list[str],
    options: Exec2Options,
) -> int:
    """Submit a job to the sandbox and return the PID.

    Args:
        sandbox: The sandbox environment to run the command in.
        cmd: Command and arguments to execute.
        options: Execution options.

    Returns:
        The process ID of the submitted job.
    """
    shell_cmd = _build_shell_command(cmd, options)

    transport = SandboxJSONRPCTransport(sandbox, SANDBOX_TOOLS_CLI)
    server_error_mapper = SandboxToolsServerErrorMapper()

    result = await exec_model_request(
        method="exec_async_submit",
        params={"command": shell_cmd},
        result_type=_SubmitResult,
        transport=transport,
        server_error_mapper=server_error_mapper,
        timeout=RPC_TIMEOUT,
        user=options.user,
    )
    return result.pid


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
        Exec2Process handle with events iterator and kill() method.
    """
    options = options or Exec2Options()
    pid = await _submit_job(sandbox, cmd, options)
    poll_interval = options.poll_interval or DEFAULT_POLL_INTERVAL

    return Exec2Process(
        sandbox=sandbox,
        pid=pid,
        poll_interval=poll_interval,
        user=options.user,
    )


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

    async for event in proc.events:
        if isinstance(event, Completed):
            return ExecResultClass[str](
                success=event.success,
                returncode=event.exit_code,
                stdout=event.stdout,
                stderr=event.stderr,
            )

    # If we get here, the process was killed (no Completed event)
    return ExecResultClass[str](
        success=False,
        returncode=-1,
        stdout="",
        stderr="",
    )
