# exec2 / exec_async Feature Plan

## Overview

Add an `exec2` method to `SandboxEnvironment` for asynchronous execution of long-running commands. Unlike `exec` which blocks until completion, `exec2` starts the process immediately and provides streaming output via an async iterator - avoiding timeout and connectivity issues with long-running commands in K8s/Docker environments.

## Naming Convention

| Context | Name | Example |
|---------|------|---------|
| **Host-side API** (SandboxEnvironment method) | `exec2` | `sandbox.exec2(["make", "build"])` |
| **Sandbox-side** (CLI, JSON-RPC, server code) | `exec_async` | `exec_async_submit`, `_exec_async/` |

The host-side uses `exec2` as a short, familiar name (parallel to `exec`). The sandbox-side uses `exec_async` to be more descriptive in the internal implementation.

## Decisions Made

- **stdout/stderr**: Separate streams (not combined like bash_session's PTY)
- **Job cleanup**: Auto-cleanup after `poll` returns a terminal status (completed/failed/killed)
- **Server restarts**: Jobs do not survive server restarts (in-memory storage)
- **Client-side Tool**: Not needed - this is server-side/CLI only
- **Process lifecycle**: `exec2()` **immediately starts** the process - it's "hot" from creation
- **Streaming only**: `Exec2Process` is async-iterable only (no await support) - keeps API simple
- **`kill()` semantics**: Calling `kill()` indicates the caller is uninterested in output or exit code. Any buffered data is discarded. If the caller wants output from a process that may have already completed, they should `poll()` instead.

---

## Execution Contexts

This feature spans three distinct execution contexts:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  INSPECT_AI PROCESS (host machine)                                          │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Code: src/inspect_ai/util/_sandbox/                                        │
│  - SandboxEnvironment.exec2() method                                        │
│  - Exec2Process class (dual-mode handle)                                    │
│  - Event types (StdoutChunk, StderrChunk, Completed)                        │
│  - Polling loop that calls sandbox.exec() to invoke CLI                     │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ sandbox.exec("inspect_sandbox_tools exec_async ...")
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SANDBOX CONTAINER                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  CLI LAYER (stateless, short-lived process)                           │  │
│  │  ─────────────────────────────────────────────────────────────────────│  │
│  │  Code: src/inspect_sandbox_tools/.../cli/main.py                      │  │
│  │  - Parses: exec_async submit|poll|kill                                │  │
│  │  - Forwards JSON-RPC request to server via Unix socket                │  │
│  │  - Returns JSON-RPC response to stdout                                │  │
│  │  - Starts server if not running                                       │  │
│  │  - Lifetime: single request/response, then exits                      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                  │                                          │
│                                  │ Unix socket JSON-RPC                     │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  SERVER LAYER (stateful, long-running process)                        │  │
│  │  ─────────────────────────────────────────────────────────────────────│  │
│  │  Code: src/inspect_sandbox_tools/.../_remote_tools/_exec_async/       │  │
│  │  - JSON-RPC methods: exec_async_submit, exec_async_poll, exec_async_kill│
│  │  - Controller: manages Job instances, thread-safe job registry        │  │
│  │  - Job: wraps asyncio subprocess, background stdout/stderr readers    │  │
│  │  - Lifetime: persists across CLI invocations, holds job state         │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Context Summary

| Context | Location | Lifetime | State | Responsibilities |
|---------|----------|----------|-------|------------------|
| **inspect_ai process** | Host machine | Eval duration | Transient | API surface, polling orchestration, event streaming |
| **CLI layer** | Sandbox container | Single request | Stateless | Parse commands, route to server, return response |
| **Server layer** | Sandbox container | Long-running | Stateful | Job lifecycle, subprocess management, output buffering |

---

## Part 1: Server Layer (Stateful, in Sandbox)

**Location**: `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec_async/`

This code runs inside the sandbox container as part of the long-running `inspect_sandbox_tools` server process. It maintains state (running jobs, output buffers) across multiple CLI invocations.

### API

Three JSON-RPC methods exposed by the server:

| JSON-RPC Method | Input | Output |
|-----------------|-------|--------|
| `exec_async_submit` | command (string) | pid (int) |
| `exec_async_poll` | pid | state, exit_code?, stdout, stderr |
| `exec_async_kill` | pid | success/failure |

### Poll Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `state` | string | Job lifecycle state: `running`, `completed`, or `killed` |
| `exit_code` | int \| None | Process exit code. Only present when state is `completed`. |
| `stdout` | string | Standard output since last poll (incremental) |
| `stderr` | string | Standard error since last poll (incremental) |

### State Values
- `running` - job is still executing
- `completed` - job finished (check `exit_code` for success/failure: 0 = success, non-zero = failure)
- `killed` - job was terminated via kill command

### Cleanup Behavior
Job is automatically removed from the controller after a `poll` call returns a terminal state (`completed` or `killed`). Subsequent polls for that pid will return an error.

### Components

All files below are in `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec_async/`:

1. **JSON-RPC Methods** (`json_rpc_methods.py`)
   - `exec_async_submit(command)` → pid
   - `exec_async_poll(pid)` → {state, exit_code?, stdout, stderr}
   - `exec_async_kill(pid)` → success message
   - Uses `@validated_json_rpc_method` decorator (shared with bash_session)

2. **Controller** (`_controller.py`)
   - Simple `dict[int, Job]` registry keyed by PID
   - `submit(command) → pid`: create Job, store by pid
   - `poll(pid) → result`: get output, cleanup if terminal
   - `kill(pid)`: terminate job
   - No `SessionController` - PIDs are natural unique identifiers

3. **Job** (`_job.py`)
   - Wraps `asyncio.create_subprocess_shell` with separate PIPE for stdout/stderr
   - Background read tasks accumulate output into buffers
   - `poll()` returns and clears incremental output
   - `kill()` terminates subprocess gracefully then forcefully

4. **Types** (`tool_types.py`)
   - Pydantic models for request/response validation

### Files to Create (Server Layer)

```
src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec_async/
├── __init__.py
├── json_rpc_methods.py    # JSON-RPC handlers
├── _controller.py         # Job registry (simple dict keyed by PID)
├── _job.py                # Subprocess wrapper with background readers
└── tool_types.py          # Pydantic models
```

### Files to Modify (Server Layer)

- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_util/load_tools.py` - register exec_async methods

### Code Sharing Recommendation

**Summary**: ~20% direct reuse, ~30% pattern reuse, ~50% new code.

#### Reuse Directly (no changes needed)

| Component | File | How to Use |
|-----------|------|------------|
| `@validated_json_rpc_method` | `_util/json_rpc_helpers.py` | Decorate JSON-RPC handlers identically |
| `load_tools` registry | `_util/load_tools.py` | Add `"exec_async": exec_async_methods` entry |

#### Follow Same Patterns (new code, same architecture)

| Pattern | bash_session Example | exec_async Equivalent |
|---------|---------------------|----------------------|
| JSON-RPC method structure | `bash_session_new_session`, `bash_session` | `exec_async_submit`, `exec_async_poll`, `exec_async_kill` |
| Background read task | `_read_loop()` in `Process` | Two read loops (stdout + stderr) in `Job` |
| Graceful termination | terminate → kill sequence | Same pattern |
| Pydantic tool_types.py | `BashParams`, `InteractResult`, etc. | `SubmitParams`, `PollResult`, etc. |

#### Do NOT Reuse (different requirements)

| Component | Why |
|-----------|-----|
| `SessionController` | exec_async uses PIDs as natural unique identifiers, no session naming needed |
| `PseudoTerminal` | exec_async uses separate pipes, not PTY |
| `AsyncDecodedStreamReader` | PTY-specific; pipes don't need incremental UTF-8 decoding |
| `Process` class | Tied to PTY I/O, interactive bash, single combined stream |
| `Session` class | Has restart capability exec_async doesn't need |
| `TimeoutEvent` | Server-driven adaptive waits; exec_async uses client-driven polling |
| `strip_control_characters()` | PTY produces ANSI escapes; pipes produce clean output |

---

## Part 2: CLI Layer (Stateless, in Sandbox)

**No new CLI code needed.** The existing `exec` subcommand already handles JSON-RPC dispatch for any method, including exec_async methods. The host-side code constructs JSON-RPC requests and passes them via the existing path.

### CLI Usage

```bash
# Submit a new job (returns pid)
inspect_sandbox_tools exec '{"jsonrpc": "2.0", "method": "exec_async_submit", "params": {"command": "long-running-command"}, "id": 1}'

# Poll job status and get incremental output
inspect_sandbox_tools exec '{"jsonrpc": "2.0", "method": "exec_async_poll", "params": {"pid": 12345}, "id": 1}'

# Kill a running job
inspect_sandbox_tools exec '{"jsonrpc": "2.0", "method": "exec_async_kill", "params": {"pid": 12345}, "id": 1}'
```

### Data Flow

```
Host calls:  sandbox.exec(["inspect_sandbox_tools", "exec", '{"jsonrpc": "2.0", "method": "exec_async_submit", ...}'])
                │
                ▼
CLI process:   Parse JSON-RPC → route to server via Unix socket
                                                                │
                                                                ▼
Server:        exec_async_submit() → Controller.submit() → Job.create()
                                                                │
                ◄───────────────────────────────────────────────┘
CLI process:   Print JSON response → exit
                │
                ▼
Host receives: {"result": {"pid": 12345}}
```

### Files to Modify (CLI Layer)

None - the existing `exec` subcommand handles this.

---

## Part 3: inspect_ai Process (Host Machine)

**Location**: `src/inspect_ai/util/_sandbox/`

This code runs in the main inspect_ai process on the host machine. It provides the Python API that solvers and tools use, and orchestrates polling to stream events back to the caller.

### Event Types

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

### Return Type

```python
class Exec2Process:
    """Handle to a running exec2 process.

    The process starts immediately when exec2() is called - it's "hot" from creation.

    Usage patterns:

    1. Streaming: iterate over events
       proc = sandbox.exec2(["cmd"])
       async for event in proc.events:
           match event:
               case StdoutChunk(data=data): print(data)
               case Completed(exit_code=code): print(f"Done: {code}")

    2. Fire-and-forget with explicit kill:
       proxy = sandbox.exec2(["./proxy"])  # starts immediately
       # ... do other work ...
       await proxy.kill()  # terminate when done
    """

    events: AsyncIterator[Exec2Event]
    """Async iterator over events as they arrive."""

    async def kill(self) -> None:
        """Terminate the process."""
        ...
```

### Method Signature

```python
# In SandboxEnvironment ABC

def exec2(
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
        Exec2Process handle with:
        - events: AsyncIterator for streaming output
        - kill(): method to terminate the process
    """
```

Note: `exec2()` is a regular method (not async) that returns `Exec2Process`. The process is started synchronously via a blocking `exec()` call to submit the job. This allows fire-and-forget patterns without an initial await.

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
    """Interval between poll requests (defaults to 0.5 seconds)."""
```

### Implementation Details

The `Exec2Process` class internally:
1. Calls `sandbox.exec(["inspect_sandbox_tools", "exec_async", "submit", cmd])` to start the job
2. Stores the returned `pid`
3. `events` iterator polls via `sandbox.exec(["inspect_sandbox_tools", "exec_async", "poll", pid])`
4. Yields `StdoutChunk`/`StderrChunk` events for incremental output
5. Yields `Completed` event when poll returns terminal state
6. `kill()` calls `sandbox.exec(["inspect_sandbox_tools", "exec_async", "kill", pid])`

### Files to Modify (inspect_ai)

1. **environment.py** (`src/inspect_ai/util/_sandbox/environment.py`)
   - Add `Exec2Options` dataclass
   - Add event types: `StdoutChunk`, `StderrChunk`, `Completed`
   - Add `Exec2Event` type alias
   - Add `Exec2Process` class
   - Add `exec2()` method to `SandboxEnvironment` ABC

2. **__init__.py** (`src/inspect_ai/util/_sandbox/__init__.py`)
   - Export: `Exec2Options`, `Exec2Process`, `Exec2Event`, `StdoutChunk`, `StderrChunk`, `Completed`

---

## Usage Examples

### Streaming Output

```python
proc = sandbox.exec2(["pytest", "-v"])
async for event in proc.events:
    match event:
        case StdoutChunk(data=data):
            print(data, end="", flush=True)
        case StderrChunk(data=data):
            print(data, end="", file=sys.stderr, flush=True)
        case Completed(exit_code=code):
            print(f"\nTests finished with code {code}")
```

### Fire-and-Forget with Kill

```python
# Start proxy immediately (no await needed to start)
proxy = sandbox.exec2(["./model-proxy"])

# Run agent, streaming output
async for event in sandbox.exec2(["claude-code", "--task", task]):
    match event:
        case StdoutChunk(data=data):
            print(data, end="", flush=True)
        case Completed(exit_code=code):
            print(f"\nAgent finished with code {code}")

# Clean up proxy
await proxy.kill()
```

---

## Implementation Checklist

### Phase 1: Server Layer (Sandbox - Stateful) ✅ COMPLETE
- [x] Create `_exec_async/` package structure
- [x] Define Pydantic models in `tool_types.py`
- [x] Implement `Job` class with subprocess management
- [x] Implement `Controller` (simple dict registry)
- [x] Implement JSON-RPC methods
- [x] Register in `load_tools.py`

### Phase 2: CLI Layer (Sandbox - Stateless) ✅ NOT NEEDED
- [x] No new CLI code required - existing `exec` subcommand handles JSON-RPC dispatch for exec_async methods

### Phase 3: inspect_ai Process (Host) ✅ COMPLETE
- [x] Add event dataclasses (`StdoutChunk`, `StderrChunk`, `Completed`)
- [x] Add `Exec2Options` dataclass
- [x] Add `Exec2Process` class (async-iterable only)
- [x] Add `exec2()` method to SandboxEnvironment ABC
- [x] Export new types from public API

### Phase 4: Testing
- [ ] Unit tests for Job class (server layer)
- [ ] Unit tests for Controller (server layer)
- [ ] Unit tests for Exec2Process (mock CLI calls)
- [ ] Test iteration yields correct event sequence
- [ ] Test timeout handling
- [ ] Test kill functionality
- [ ] Integration test with actual sandbox

---

## Comparison: bash_session vs exec2

### Fundamental Difference: Tool vs Infrastructure

**bash_session is a Tool** - It's exposed to models/agents as a callable tool. The model can invoke `bash_session` to run commands in a persistent shell. This means:
- Has tool registration and schema
- Appears in tool listings
- Model decides when to call it
- Part of the agent's action space

**exec2 is NOT a Tool** - It's infrastructure for solver/evaluation code. The `exec2()` method is called by Python code running in the inspect_ai process, not by models. This means:
- No tool registration or schema
- Not visible to models
- Solver/tool implementation code calls it directly
- Similar to `sandbox.exec()` - a programmatic API, not an agent action

### I/O Model: PTY vs Separate Pipes

**bash_session uses a PTY (pseudo-terminal)**

A PTY emulates a real terminal device. bash_session creates a PTY pair and attaches bash's stdin/stdout/stderr all to the same PTY file descriptor:

```python
# bash_session's approach
pty = await PseudoTerminal.create()
process = await asyncio.create_subprocess_exec(
    "/bin/bash", "-i",
    stdin=pty.subprocess_fd,
    stdout=pty.subprocess_fd,   # Same fd
    stderr=pty.subprocess_fd,   # Same fd
)
```

Implications of PTY:
- **Combined streams**: stdout and stderr are interleaved in arrival order (like a real terminal)
- **Interactive shell**: Bash runs in interactive mode (`-i`), loading `.bashrc`, enabling job control
- **Line buffering**: PTY provides proper line buffering for interactive use
- **Terminal features**: Supports terminal escape sequences, though bash_session strips them
- **Complexity**: Requires PTY management, terminal attribute configuration, echo disabling
- **Use case**: Persistent shell sessions where the model sends multiple commands over time

**exec_async uses separate pipes**

exec_async creates the subprocess with independent pipes for stdout and stderr:

```python
# exec_async's approach
process = await asyncio.create_subprocess_shell(
    command,
    stdout=asyncio.subprocess.PIPE,  # Separate pipe
    stderr=asyncio.subprocess.PIPE,  # Separate pipe
)
```

Implications of separate pipes:
- **Distinct streams**: stdout and stderr are captured independently, can be processed/displayed separately
- **Non-interactive shell**: No `.bashrc`, no job control, simpler environment
- **Block buffering**: Pipes use block buffering by default (programs may buffer output until exit)
- **Simpler implementation**: No PTY setup, just standard subprocess pipes
- **Ordering caveat**: Cannot reconstruct exact interleaving of stdout/stderr (each has its own buffer)
- **Use case**: Running a single command and streaming its output back to the caller

### Comparison Table

| Aspect | bash_session | exec2 / exec_async |
|--------|--------------|------------|
| **Type** | Tool (model-callable) | Infrastructure (code-callable) |
| **Purpose** | Persistent interactive shell | One-shot command execution |
| **Session lifecycle** | Long-lived, survives calls | Single command, auto-cleanup |
| **I/O model** | PTY (combined stdout/stderr) | Separate pipes (distinct streams) |
| **Shell mode** | Interactive (`bash -i`) | Non-interactive (`sh -c`) |
| **Buffering** | Line buffered (PTY) | Block buffered (pipes) |
| **Output delivery** | Accumulated, cleared after interact | Incremental per poll |
| **Stream separation** | No (interleaved) | Yes (stdout/stderr independent) |
| **Restart capability** | Yes | No (kill and submit new) |

### Shared Infrastructure

Despite these differences, both features share underlying infrastructure in the sandbox tools server:

| Component | Used By | Notes |
|-----------|---------|-------|
| `@validated_json_rpc_method` | Both | Same decorator for JSON-RPC registration |
| JSON-RPC server | Both | Same aiohttp server process |
| Unix socket communication | Both | Same IPC mechanism |
| CLI dispatch pattern | Both | Same routing in `main.py` |

### What exec_async Does NOT Share

| Component | Why Not Shared |
|-----------|----------------|
| `SessionController` | exec_async uses PIDs as natural unique identifiers, no session naming needed |
| `PseudoTerminal` | exec_async uses pipes, not PTY |
| `AsyncDecodedStreamReader` | PTY-specific UTF-8 handling not needed |
| `Process` class | Deeply tied to PTY I/O and interactive bash |
| `Session` class | Has restart capability exec_async doesn't need |
| `TimeoutEvent` | bash_session's adaptive wait; exec_async uses client-driven polling |
| `strip_control_characters()` | PTY produces ANSI escapes; pipes don't |

---

## Open Questions

- [ ] **Output buffering strategy**: Currently planned as incremental (data since last poll). Should we also support a mode that buffers all output? Or is that just "await the process"?

## Future Cleanup

- [ ] **Rename `_remote_tools` directory**: The name `_remote_tools` is misleading now that it contains `_exec_async`, which is infrastructure rather than a tool. Consider renaming to `_remote_services` or `_json_rpc_services` to better reflect that it contains both tools (like `bash_session`) and infrastructure (like `exec_async`). Add a TODO comment in the code when creating the `_exec_async` directory.

- [ ] **Replace `ToolException` usage**: `exec_async` uses `ToolException` for error handling, but since exec_async isn't a tool, this is semantically incorrect. Consider creating a more general exception type (e.g., `ServiceException` or `JsonRpcException`) or using a standard exception type.

---

## Verification

1. **Unit test**: Mock sandbox.exec() calls, verify streaming behavior
2. **Integration test**: Run actual long-running command in Docker sandbox
3. **Manual test**:
   ```python
   sandbox = await get_sandbox()

   # Test streaming
   proc = sandbox.exec2(["bash", "-c", "for i in 1 2 3; do echo $i; sleep 1; done"])
   async for event in proc.events:
       print(f"Event: {event}")

   # Test fire-and-forget with kill
   proxy = sandbox.exec2(["sleep", "999"])
   await asyncio.sleep(1)
   await proxy.kill()
   print("Proxy killed")
   ```
