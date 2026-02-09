from ..._util.json_rpc_helpers import validated_json_rpc_method
from ._controller import Controller
from .tool_types import (
    KillParams,
    KillResult,
    PollParams,
    PollResult,
    SubmitParams,
    SubmitResult,
)

controller = Controller()


@validated_json_rpc_method(SubmitParams)
async def exec_async_submit(params: SubmitParams) -> SubmitResult:
    """Submit a command for async execution. Returns the PID."""
    pid = await controller.submit(params.command)
    return SubmitResult(pid=pid)


@validated_json_rpc_method(PollParams)
async def exec_async_poll(params: PollParams) -> PollResult:
    """Poll job state and get incremental output."""
    return await controller.poll(params.pid)


@validated_json_rpc_method(KillParams)
async def exec_async_kill(params: KillParams) -> KillResult:
    """Kill a running job."""
    return await controller.kill(params.pid)
