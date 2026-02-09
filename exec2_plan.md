# exec2 Feature Plan

## Overview

Add an `exec2` capability for asynchronous execution of long-running commands. Unlike `exec` which blocks until completion, `exec2` starts the process immediately and provides streaming output via an async iterator - avoiding timeout and connectivity issues with long-running commands in K8s/Docker environments.

## Decisions Made

- **stdout/stderr**: Separate streams (not combined like bash_session's PTY)
- **Job cleanup**: Auto-cleanup after `poll` returns a terminal status (completed/failed/killed)
- **Server restarts**: Jobs do not survive server restarts (in-memory storage)
- **Client-side Tool**: Not needed - this is server-side/CLI only
- **Process lifecycle**: `exec2()` **immediately starts** the process - it's "hot" from creation
- **Dual-mode return**: `Exec2Process` is both awaitable (for final result) and async-iterable (for streaming)

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
│  CLI / JSON-RPC Layer (sandbox tools server)                │
│    - exec2 submit → job_id                                  │
│    - exec2 poll → {state, exit_code?, stdout, stderr}       │
│    - exec2 kill → success/failure                           │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│  Job Controller + Job (in sandbox tools server)             │
│    - Manages job lifecycle and cleanup                      │
│    - Wraps asyncio subprocess                               │
│    - Background read tasks for stdout/stderr                │
└─────────────────────────────────────────────────────────────┘
```

---

## Part 1: CLI / JSON-RPC Layer (Sandbox Tools Server)

### API

Three operations with a simplified, combined status+output design:

| Operation | CLI | JSON-RPC Method | Input | Output |
|-----------|-----|-----------------|-------|--------|
| **submit** | `exec2 submit <command>` | `exec2_submit` | command (string) | job_id (string) |
| **poll** | `exec2 poll <job_id>` | `exec2_poll` | job_id | state, exit_code?, stdout, stderr |
| **kill** | `exec2 kill <job_id>` | `exec2_kill` | job_id | success/failure |

### Poll Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `state` | string | Job lifecycle state: `running`, `completed`, or `killed` |
| `exit_code` | int \| None | Process exit code (0 = success, non-zero = failure). Only present when state is `completed`. |
| `stdout` | string | Standard output captured from the process |
| `stderr` | string | Standard error captured from the process |

### State Values
- `running` - job is still executing
- `completed` - job finished (check `exit_code` for success/failure: 0 = success, non-zero = failure)
- `killed` - job was terminated via kill command

### Cleanup Behavior
Job is automatically removed from the controller after a `poll` call returns a terminal state (`completed` or `killed`). Subsequent polls for that job_id will return an error.

### Components

1. **CLI Layer** (`main.py`)
   - New `exec2` subcommand with sub-subcommands: `submit`, `poll`, `kill`
   - Routes to JSON-RPC methods via Unix socket to server

2. **JSON-RPC Methods** (`_remote_tools/_exec2/json_rpc_methods.py`)
   - `exec2_submit(command)` → job_id
   - `exec2_poll(job_id)` → {state, exit_code?, stdout, stderr}
   - `exec2_kill(job_id)` → success message

3. **Job Controller** (`_remote_tools/_exec2/_controller.py`)
   - Manages job lifecycle and cleanup
   - Thread-safe job storage (similar to SessionController pattern)

4. **Job Class** (`_remote_tools/_exec2/_job.py`)
   - Wraps asyncio subprocess (using `asyncio.create_subprocess_shell`)
   - Background read tasks for stdout and stderr (separate pipes, not PTY)
   - Tracks status and exit code

### Data Flow

```
CLI: exec2 submit "long-running-command"
    → JSON-RPC: exec2_submit(command="long-running-command")
    → Controller.submit(command)
    → Job.create(command)  # spawns subprocess
    → returns job_id (e.g., "job_0")

CLI: exec2 poll job_0
    → JSON-RPC: exec2_poll(job_id="job_0")
    → Controller.poll(job_id)
    → Job.poll()  # gets current state
    → returns {state: "running", stdout: "...", stderr: "..."}

CLI: exec2 poll job_0  (after completion)
    → JSON-RPC: exec2_poll(job_id="job_0")
    → Controller.poll(job_id)
    → returns {state: "completed", exit_code: 0, stdout: "...", stderr: "..."}
    → Controller removes job from storage (auto-cleanup)

CLI: exec2 kill job_0
    → JSON-RPC: exec2_kill(job_id="job_0")
    → Controller.kill(job_id)
    → Job.kill()  # terminates subprocess
    → returns success message
```

### Files to Create (CLI Layer)

- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/__init__.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/json_rpc_methods.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/_controller.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/_job.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/tool_types.py`

### Files to Modify (CLI Layer)

- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_cli/main.py` - add exec2 subcommand
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_util/load_tools.py` - register exec2 methods

---

## Part 2: SandboxEnvironment API (Client Side)

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

### Files to Modify (Client Side)

1. **environment.py** (`src/inspect_ai/util/_sandbox/environment.py`)
   - Add `Exec2Options` dataclass
   - Add event types: `StdoutChunk`, `StderrChunk`, `Completed`
   - Add `Exec2Event` type alias
   - Add `Exec2Process` class
   - Add `exec2()` method to `SandboxEnvironment` ABC
   - Add private helpers: `_exec2_submit`, `_exec2_poll`, `_exec2_kill`

2. **__init__.py** (`src/inspect_ai/util/_sandbox/__init__.py`)
   - Export: `Exec2Options`, `Exec2Process`, `Exec2Event`, `StdoutChunk`, `StderrChunk`, `Completed`

---

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

---

## Implementation Checklist

### Phase 1: CLI Layer (Sandbox Tools Server)
- [ ] Create `_exec2/` package structure
- [ ] Define Pydantic models in `tool_types.py`
- [ ] Implement `Job` class with subprocess management
- [ ] Implement `Controller` extending `SessionController`
- [ ] Implement JSON-RPC methods (poll returns incremental output)
- [ ] Register in `load_tools.py`
- [ ] Add CLI subcommand parsing in `main.py`
- [ ] Add CLI dispatch logic

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
- [ ] Unit tests for Job class
- [ ] Unit tests for Controller
- [ ] Unit tests for Exec2Process (mock CLI)
- [ ] Test await mode returns ExecResult
- [ ] Test iteration mode yields correct event sequence
- [ ] Test timeout handling
- [ ] Test cancellation (job gets killed)
- [ ] Integration test with actual sandbox

---

## Open Questions

- [ ] **Output buffering strategy**: Should `poll` return incremental stdout/stderr (data since last poll), or buffer all output until subprocess completes? Need to understand caller/use cases better before deciding.

---

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
