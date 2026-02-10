import asyncio
import os
import signal
from asyncio.subprocess import Process as AsyncIOProcess
from typing import Literal

from .tool_types import PollResult


class Job:
    """Manages an async subprocess with separate stdout/stderr streams.

    The Job wraps asyncio.create_subprocess_shell with PIPE for stdout/stderr.
    Background read tasks accumulate output into buffers. poll() returns and
    clears incremental output. kill() terminates the subprocess gracefully
    then forcefully.
    """

    @classmethod
    async def create(cls, command: str) -> "Job":
        """Create and start a new Job for the given command.

        Uses start_new_session=True so the subprocess becomes its own process
        group leader. This allows kill() to terminate the entire process tree
        (including any child processes spawned by the command).
        """
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return cls(process)

    def __init__(self, process: AsyncIOProcess) -> None:
        self._process = process
        self._stdout_buffer: list[str] = []
        self._stderr_buffer: list[str] = []
        self._state: Literal["running", "completed", "killed"] = "running"
        self._exit_code: int | None = None

        # Start background read tasks
        self._stdout_task = asyncio.create_task(
            self._read_stream(process.stdout, self._stdout_buffer)
        )
        self._stderr_task = asyncio.create_task(
            self._read_stream(process.stderr, self._stderr_buffer)
        )

    @property
    def pid(self) -> int:
        """Return the process ID."""
        assert self._process.pid is not None
        return self._process.pid

    async def poll(self) -> PollResult:
        """Return current state and incremental output, clearing buffers."""
        # Check if process has finished
        if self._state == "running" and self._process.returncode is not None:
            self._state = "completed"
            self._exit_code = self._process.returncode
            # Wait for read tasks to finish draining
            await self._wait_for_readers()

        # Collect and clear buffers
        stdout = "".join(self._stdout_buffer)
        stderr = "".join(self._stderr_buffer)
        self._stdout_buffer.clear()
        self._stderr_buffer.clear()

        return PollResult(
            state=self._state,
            exit_code=self._exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    async def kill(self, timeout: int = 5) -> None:
        """Terminate the process and its entire process group.

        Since the subprocess was started with start_new_session=True, it is the
        leader of its own process group. We use os.killpg() to send signals to
        the entire group, ensuring child processes are also terminated.
        """
        if self._state != "running":
            return

        self._state = "killed"
        pgid = self._process.pid

        # Try graceful termination first (SIGTERM to process group)
        try:
            os.killpg(pgid, signal.SIGTERM)
            await asyncio.wait_for(self._process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # Force kill if graceful termination times out (SIGKILL to process group)
            os.killpg(pgid, signal.SIGKILL)
            await self._process.wait()
        except ProcessLookupError:
            # Process already exited
            pass

        await self._wait_for_readers()

    async def cleanup(self) -> None:
        """Clean up resources. Called after job is removed from controller."""
        await self._wait_for_readers()

    async def _wait_for_readers(self) -> None:
        """Wait for background read tasks to complete."""
        for task in [self._stdout_task, self._stderr_task]:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _read_stream(
        self, stream: asyncio.StreamReader | None, buffer: list[str]
    ) -> None:
        """Read from a stream and append to buffer."""
        if stream is None:
            return

        try:
            while True:
                data = await stream.read(4096)
                if not data:
                    break
                buffer.append(data.decode("utf-8", errors="replace"))
        except asyncio.CancelledError:
            pass
