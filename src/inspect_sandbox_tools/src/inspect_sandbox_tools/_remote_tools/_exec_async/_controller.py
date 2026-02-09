from inspect_sandbox_tools._util.common_types import ToolException

from ._job import Job
from .tool_types import KillResult, PollResult


class Controller:
    """Simple job registry keyed by PID.

    Unlike bash_session's SessionController, exec_async uses PIDs as natural
    unique identifiers - no session naming or multiplexing needed.
    """

    def __init__(self) -> None:
        self._jobs: dict[int, Job] = {}

    async def submit(self, command: str) -> int:
        """Create a new job and return its PID."""
        job = await Job.create(command)
        self._jobs[job.pid] = job
        return job.pid

    async def poll(self, pid: int) -> PollResult:
        """Get job state and incremental output. Auto-cleanup on terminal state."""
        job = self._get_job(pid)
        result = await job.poll()

        # Auto-cleanup after terminal state
        if result.state in ("completed", "killed"):
            del self._jobs[pid]
            await job.cleanup()

        return result

    async def kill(self, pid: int) -> KillResult:
        """Terminate a running job."""
        job = self._get_job(pid)
        await job.kill()
        # Clean up the job after killing
        del self._jobs[pid]
        await job.cleanup()
        return KillResult(message=f"Job {pid} killed")

    def _get_job(self, pid: int) -> Job:
        """Get job by PID or raise error."""
        job = self._jobs.get(pid)
        if job is None:
            raise ToolException(f"No job found with pid {pid}")
        return job
