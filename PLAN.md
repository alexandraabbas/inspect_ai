# exec2 Feature Plan

## Overview

Add an `exec2` subcommand to the sandbox tools CLI that supports asynchronous execution of long-running commands. Unlike the current `exec` which blocks until completion, `exec2` allows callers to submit jobs, poll for status/output, and retrieve results later - avoiding timeout and connectivity issues with long-running commands in K8s/Docker environments.

## Decisions Made

- **stdout/stderr**: Separate streams (not combined like bash_session's PTY)
- **Job cleanup**: Auto-cleanup after `poll` returns a terminal status (completed/failed/killed)
- **Server restarts**: Jobs do not survive server restarts (in-memory storage)
- **Client-side Tool**: Not needed - this is server-side/CLI only

## API

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

## Open Questions

- [ ] **Output buffering strategy**: Should `poll` return incremental stdout/stderr (data since last poll), or buffer all output until subprocess completes? Need to understand caller/use cases better before deciding.

## Architecture

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

## Implementation Checklist

- [ ] Create `_remote_tools/_exec2/` package structure
- [ ] Define Pydantic models in `tool_types.py`
- [ ] Implement `Job` class with subprocess management
- [ ] Implement `Controller` extending `SessionController`
- [ ] Implement JSON-RPC methods
- [ ] Register in `load_tools.py`
- [ ] Add CLI subcommand parsing in `main.py`
- [ ] Add CLI dispatch logic
- [ ] Write tests

## Files to Create/Modify

### New Files
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/__init__.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/json_rpc_methods.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/_controller.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/_job.py`
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_remote_tools/_exec2/tool_types.py`

### Modified Files
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_cli/main.py` - add exec2 subcommand
- `src/inspect_sandbox_tools/src/inspect_sandbox_tools/_util/load_tools.py` - register exec2 methods
