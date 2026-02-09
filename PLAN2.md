# exec2 Feature Plan (Revised)

## Overview

Add an `exec2` method to the `SandboxEnvironment` ABC that supports asynchronous execution of long-running commands. Unlike `exec` which blocks until completion, `exec2` starts the process immediately and provides streaming output via an async iterator.

**Key insights**:
- `exec2` is exposed as a method on `SandboxEnvironment` with a single implementation in the ABC itself (not abstract)
- Calling `exec2()` **immediately starts** the process - it's "hot" from the moment of creation
- Iteration/await is for **consuming output**, not for starting the process
- `Exec2Process.kill()` can be called anytime to terminate the process

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Caller (solver, tool, etc.)                                │
│    result = await sandbox.exec2(cmd, options)    # simple   │
│    async for event in sandbox.exec2(...): ...    # stream   │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  SandboxEnvironment.exec2() [single implementation in ABC]  │
│    - Returns Exec2Process (dual-mode: awaitable + iterable) │
│    - Calls CLI: exec2 submit <command>                      │
│    - Polls: exec2 poll <job_id>                             │
│    - Yields typed events (StdoutChunk, StderrChunk, etc.)   │
└────────────────────────────┬────────────────────────────────┘
                             │ (via existing sandbox exec mechanism)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  CLI / JSON-RPC Layer (already spec'd in PLAN.md)           │
│    - exec2 submit → job_id                                  │
│    - exec2 poll → {state, exit_code?, stdout, stderr}       │
│    - exec2 kill → success/failure                           │
└─────────────────────────────────────────────────────────────┘
```

## API Design

### Event Types (Union of Dataclasses)

```python
from dataclasses import dataclass
from typing import Union

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
    """Process completed (successfully or with error)."""
    exit_code: int
    stdout: str  # Full accumulated stdout
    stderr: str  # Full accumulated stderr

    @property
    def success(self) -> bool:
        return self.exit_code == 0

Exec2Event = Union[StdoutChunk, StderrChunk, Completed]
```

### Dual-Mode Return Type

```python
class Exec2Process:
    """Handle to a running exec2 process.

    The process starts immediately when exec2() is called - it's "hot" from creation.
    Iteration/await is for consuming output, not for starting the process.

    This object supports three usage patterns:

    1. Simple (like exec): await for final result
       result = await sandbox.exec2(["cmd"])
       print(result.stdout)

    2. Streaming: iterate for real-time events
       async for event in sandbox.exec2(["cmd"]):
           match event:
               case StdoutChunk(data=data): print(data)
               case Completed(exit_code=code): print(f"Done: {code}")

    3. Fire-and-forget with explicit kill:
       proxy = sandbox.exec2(["./proxy"])  # starts immediately
       # ... do other work ...
       await proxy.kill()  # terminate when done
    """

    def __await__(self) -> Generator[Any, None, ExecResult[str]]:
        """Await for final result (consumes all events internally)."""
        ...

    def __aiter__(self) -> AsyncIterator[Exec2Event]:
        """Iterate over events as they arrive."""
        ...

    async def kill(self) -> None:
        """Terminate the process.

        Can be called at any time after exec2() returns.
        Safe to call multiple times or after process has completed (no-op).
        """
        ...
```

### Method Signature

```python
# In SandboxEnvironment ABC (environment.py)

async def exec2(
    self,
    cmd: list[str],
    options: Exec2Options | None = None,
) -> Exec2Process:
    """Start a long-running command and return a handle to it.

    The process starts immediately when this method is called.
    Unlike exec(), exec2 does not block waiting for completion.

    Args:
        cmd: Command and arguments to execute.
        options: Execution options (see Exec2Options).

    Returns:
        Exec2Process handle that can be:
        - Awaited for final ExecResult (like exec)
        - Iterated for streaming events
        - Killed via kill() method

    Example (simple):
        result = await (await sandbox.exec2(["make", "build"]))
        # Or more naturally:
        proc = await sandbox.exec2(["make", "build"])
        result = await proc

    Example (streaming):
        proc = await sandbox.exec2(["make", "build"])
        async for event in proc:
            match event:
                case StdoutChunk(data=data):
                    print(data, end="")
                case Completed(exit_code=code):
                    print(f"\\nBuild finished with code {code}")

    Example (fire-and-forget):
        proxy = await sandbox.exec2(["./proxy"])  # starts immediately
        # ... do other work ...
        await proxy.kill()  # terminate when done
    """
```

### Options Object

```python
@dataclass
class Exec2Options:
    """Options for exec2() command execution."""

    input: str | bytes | None = None
    """Standard input to send to the command."""

    cwd: str | None = None
    """Working directory for command execution."""

    env: dict[str, str] | None = None
    """Additional environment variables."""

    user: str | None = None
    """User to run the command as."""

    timeout: int | None = None
    """Maximum execution time in seconds."""

    poll_interval: float | None = None
    """Interval between poll requests (defaults to sandbox's default_polling_interval())."""
```

### Comparison with exec()

| Aspect | `exec()` | `exec2()` |
|--------|----------|-----------|
| Parameters | 8 individual params | 1 options object |
| Return | `ExecResult[str]` | `Exec2Process` (awaitable + iterable) |
| Blocking | Blocks until complete | Blocks but polls internally |
| Output | Final result only | Stream events OR final result |
| Timeout handling | `timeout_retry` param | No retry, just timeout |
| Concurrency | `concurrency` param | Not needed (polling is lightweight) |
| Implementation | Abstract (per-sandbox) | Single impl in ABC |
| Binary mode | Supported (text=False) | Text only |

## Usage Examples

### Simple Usage (Migration from exec)

```python
# Before (exec)
result = await sandbox.exec(["make", "build"], timeout=300)

# After (exec2) - nearly identical
result = await sandbox.exec2(["make", "build"], Exec2Options(timeout=300))
```

### Streaming Usage

```python
async for event in sandbox.exec2(["pytest", "-v"]):
    match event:
        case StdoutChunk(data=data):
            # Print test output in real-time
            print(data, end="", flush=True)
        case StderrChunk(data=data):
            print(data, end="", file=sys.stderr, flush=True)
        case Completed(exit_code=code, stdout=out, stderr=err):
            print(f"\nTests finished with code {code}")
            # Full output also available here if needed
```

### With Options

```python
options = Exec2Options(
    cwd="/app",
    env={"DEBUG": "1"},
    timeout=600,
    user="appuser",
)
result = await sandbox.exec2(["./long-running-script.sh"], options)
```

### Agent Bridge Pattern (Fire-and-Forget + Kill)

```python
# Start proxy immediately (no await needed to start)
proxy = sandbox.exec2(["./model-proxy"])
# proxy is already running in the sandbox

# Run the claude code agent, streaming its output
async for event in sandbox.exec2(["claude-code", "--task", task]):
    match event:
        case StdoutChunk(data=data):
            # Stream agent output to user in real-time
            print(data, end="", flush=True)
        case Completed(exit_code=code):
            print(f"\nAgent finished with code {code}")

# Clean up the proxy when done
await proxy.kill()
```

## Implementation Details

### Exec2Process Implementation

```python
class Exec2Process:
    def __init__(
        self,
        sandbox: "SandboxEnvironment",
        job_id: str,  # Already submitted - process is running
        poll_interval: float,
        timeout: float | None,
    ):
        self._sandbox = sandbox
        self._job_id = job_id
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._iterating = False
        self._completed = False
        self._killed = False
        self._stdout_buffer: list[str] = []
        self._stderr_buffer: list[str] = []

    def __await__(self):
        return self._await_impl().__await__()

    async def _await_impl(self) -> ExecResult[str]:
        """Consume all events and return final result."""
        async for event in self:
            if isinstance(event, Completed):
                return ExecResult(
                    success=event.success,
                    returncode=event.exit_code,
                    stdout=event.stdout,
                    stderr=event.stderr,
                )
        raise RuntimeError("Process ended without Completed event")

    async def __aiter__(self) -> AsyncIterator[Exec2Event]:
        if self._iterating:
            raise RuntimeError("Exec2Process can only be iterated once")
        self._iterating = True

        start_time = time.monotonic()

        try:
            while True:
                # Check timeout
                if self._timeout:
                    elapsed = time.monotonic() - start_time
                    if elapsed >= self._timeout:
                        await self.kill()
                        raise TimeoutError(f"exec2 timed out after {self._timeout}s")

                # Poll for status
                poll_result = await self._sandbox._exec2_poll(self._job_id)

                # Yield stdout chunks
                if poll_result.stdout:
                    self._stdout_buffer.append(poll_result.stdout)
                    yield StdoutChunk(data=poll_result.stdout)

                # Yield stderr chunks
                if poll_result.stderr:
                    self._stderr_buffer.append(poll_result.stderr)
                    yield StderrChunk(data=poll_result.stderr)

                # Check for completion
                if poll_result.state in ("completed", "killed"):
                    self._completed = True
                    yield Completed(
                        exit_code=poll_result.exit_code or -1,
                        stdout="".join(self._stdout_buffer),
                        stderr="".join(self._stderr_buffer),
                    )
                    return

                await asyncio.sleep(self._poll_interval)

        except asyncio.CancelledError:
            if not self._completed:
                await self.kill()
            raise

    async def kill(self) -> None:
        """Terminate the process.

        Can be called at any time after exec2() returns.
        Safe to call multiple times or after process has completed (no-op).
        """
        if self._killed or self._completed:
            return
        self._killed = True
        await self._sandbox._exec2_kill(self._job_id)
```

### SandboxEnvironment.exec2 Implementation

```python
async def exec2(
    self,
    cmd: list[str],
    options: Exec2Options | None = None,
) -> Exec2Process:
    """Start a process immediately and return a handle to it.

    The process begins running as soon as this method returns.
    Use the returned Exec2Process to:
    - await it for final result
    - iterate for streaming output
    - call kill() to terminate
    """
    opts = options or Exec2Options()
    poll_interval = opts.poll_interval or self.default_polling_interval()

    # Submit immediately - process starts running now
    job_id = await self._exec2_submit(cmd, opts)

    return Exec2Process(
        sandbox=self,
        job_id=job_id,
        poll_interval=poll_interval,
        timeout=opts.timeout,
    )
```

Note: `exec2()` is an async method because it submits the job immediately. The process is running by the time `Exec2Process` is returned.

## Files to Modify

### Modified Files

1. **[environment.py](src/inspect_ai/util/_sandbox/environment.py)**
   - Add `Exec2Options` dataclass
   - Add event types: `StdoutChunk`, `StderrChunk`, `Completed`
   - Add `Exec2Event` type alias
   - Add `Exec2Process` class
   - Add `exec2()` method to `SandboxEnvironment` ABC
   - Add private helpers: `_exec2_submit`, `_exec2_poll`, `_exec2_kill`

2. **[__init__.py](src/inspect_ai/util/_sandbox/__init__.py)**
   - Export: `Exec2Options`, `Exec2Process`, `Exec2Event`, `StdoutChunk`, `StderrChunk`, `Completed`

### New Files (CLI layer - from existing PLAN.md)

- `src/inspect_sandbox_tools/.../exec2/` package

## Implementation Checklist

### Phase 1: CLI Layer (from existing PLAN.md)
- [ ] Create `_exec2/` package structure
- [ ] Implement Job class with subprocess management
- [ ] Implement Controller
- [ ] Implement JSON-RPC methods (poll returns incremental output)
- [ ] Add CLI subcommand parsing

### Phase 2: SandboxEnvironment API
- [ ] Add event dataclasses (`StdoutChunk`, `StderrChunk`, `Completed`)
- [ ] Add `Exec2Options` dataclass
- [ ] Add `Exec2Process` class with dual-mode support
- [ ] Add `exec2()` method to SandboxEnvironment ABC
- [ ] Implement `_exec2_submit()` helper
- [ ] Implement `_exec2_poll()` helper
- [ ] Implement `_exec2_kill()` helper
- [ ] Export new types from public API

### Phase 3: Testing
- [ ] Unit tests for Exec2Process (mock CLI)
- [ ] Test await mode returns ExecResult
- [ ] Test iteration mode yields correct event sequence
- [ ] Test timeout handling
- [ ] Test cancellation (job gets killed)
- [ ] Integration test with actual sandbox

## Verification

1. **Unit test**: Mock CLI responses, verify dual-mode behavior
2. **Integration test**: Run actual long-running command
3. **Manual test**:
   ```python
   sandbox = await get_sandbox()

   # Test streaming
   proc = await sandbox.exec2(["bash", "-c", "for i in 1 2 3; do echo $i; sleep 1; done"])
   async for event in proc:
       print(f"Event: {event}")

   # Test simple await
   proc = await sandbox.exec2(["echo", "hello"])
   result = await proc
   print(f"Result: {result}")

   # Test fire-and-forget with kill
   proxy = await sandbox.exec2(["sleep", "999"])
   await asyncio.sleep(1)  # let it run briefly
   await proxy.kill()
   print("Proxy killed")
   ```
