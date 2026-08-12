# Backend Agent Instructions

> Full system architecture diagram: `docs/architecture.md`

## Framework & Stack

- **FastAPI** (async), **SQLAlchemy** ORM with **SQLite**, **Pydantic** validation
- Entry: `main.py` (app lifecycle, middleware, WebSocket, auth, startup/shutdown)
- DB file: `/config/journal.db`; models in `models.py`

## Directory Structure

```
backend/
├── main.py                 # App entry, middleware, router registration
├── routers/                # 20 domain-focused API routers
│   └── __init__.py         # all_routers list (registration order matters)
├── services/               # Service layer (notification_service.py)
├── tasks/                  # Scheduled task implementations
├── auth/                   # Auth subsystem (routes, tokens, dependencies, providers/)
├── tls/                    # TLS/ACME certificate management
├── tests/                  # Test suite (conftest.py, routers/, services/, unit/, integration/)
├── models.py               # SQLAlchemy ORM models
├── database.py             # Session factory, init_db()
├── config.py               # Settings management
├── dispatcharr_client.py   # Async HTTP client for Dispatcharr API
├── journal.py              # Audit logging
├── cache.py                # In-memory cache with TTL
├── auto_creation_*.py      # Auto-creation engine/evaluator/executor/schema
├── stream_prober.py        # Stream health checking
├── task_scheduler.py       # Abstract task base class
├── task_registry.py        # Task registry
└── task_engine.py          # Task execution engine
```

## Router Conventions

```python
router = APIRouter(prefix="/api/channels", tags=["Channels"])

@router.get("")            # Root route uses "" not "/"
async def get_channels(...):
```

- Prefix format: `/api/<domain>` (e.g., `/api/channels`, `/api/m3u`, `/api/settings`)
- Root routes use `""` (empty string), NOT `"/"` — avoids trailing slash 307 redirects
- Tags match domain names: `tags=["Channels"]`, `tags=["M3U"]`, `tags=["Settings"]`
- Routers registered in `routers/__init__.py` → `all_routers` list → included by `main.py`

## Logging

```python
import logging
logger = logging.getLogger(__name__)

# Always use lazy % formatting, never f-strings in log calls
logger.info("[CHANNELS] Created channel id=%s name=%s", channel_id, name)
logger.warning("[CHANNELS] Failed to update: %s", e)
logger.debug("[CHANNELS] Fetched %d channels in %.1fms", count, elapsed_ms)
```

- **Prefix format**: `[UPPERCASE-MODULE]` in brackets (e.g., `[CHANNELS]`, `[M3U]`, `[EPG]`, `[AUTH]`, `[TASKS]`, `[DATABASE]`)
- **Lazy formatting**: Always `logger.x("msg %s", val)` — never `logger.x(f"msg {val}")`

## Error Handling

```python
# Standard pattern in routers
try:
    result = await client.operation()
except Exception as e:
    logger.warning("[MODULE] Operation failed: %s", e)
    raise HTTPException(status_code=500, detail=str(e))
```

- Never silently swallow exceptions (`except: pass`)
- Always log before raising HTTPException
- Status codes: 200 (success), 204 (delete), 400 (validation), 404 (not found), 409 (conflict), 500 (server error)

## Database Patterns

```python
from database import get_session

# In routers - FastAPI dependency injection
@router.get("/items")
async def get_items(db: Session = Depends(get_session)):
    ...

# In tasks/services - direct usage
db = get_session()
try:
    ...
finally:
    db.close()
```

## Testing

- Run: `cd backend && python -m pytest tests/ --tb=short --no-header -p no:warnings 2>&1 | tail -1`
- This command suppresses warnings and headers so `tail -1` reliably returns the summary (e.g., `2147 passed in 50s`). Do NOT use `-q` — it suppresses the summary line when all tests pass.
- In-memory SQLite with `StaticPool` for isolation
- **Mock at router module level**: `patch("routers.channels.get_client", ...)` — NOT `patch("main.get_client", ...)`
- Fixtures in `tests/conftest.py`: `test_engine`, `test_session`, `async_client`
- Test naming: `test_returns_channels()`, `test_client_error()`, `test_creates_item()`

## Task System

```python
from task_scheduler import TaskScheduler, TaskResult
from task_registry import register_task

class M3URefreshTask(TaskScheduler):
    task_id = "m3u_refresh"
    name = "M3U Refresh"

    async def execute(self) -> TaskResult:
        ...

register_task(M3URefreshTask)
```

## Key Singletons

- `get_client()` / `reset_client()` — Dispatcharr HTTP client
- `get_settings()` / `save_settings()` — Configuration
- `get_cache()` — In-memory cache with TTL
- `journal.log_entry(category, action_type, ...)` — Audit logging

## Deploy to Container

```bash
docker cp backend/main.py ecm-ecm-1:/app/main.py
docker cp backend/routers/. ecm-ecm-1:/app/routers/
docker restart ecm-ecm-1   # No --reload; restart required
```

Backend deploys to `/app/` (NOT `/app/backend/`). The entrypoint runs `cd /app && uvicorn main:app`.

# context-mode — MANDATORY routing rules

You have context-mode MCP tools available. These rules are NOT optional — they protect your context window from flooding. A single unrouted command can dump 56 KB into context and waste the entire session.

## BLOCKED commands — do NOT attempt these

### curl / wget — BLOCKED
Any Bash command containing `curl` or `wget` is intercepted and replaced with an error message. Do NOT retry.
Instead use:
- `ctx_fetch_and_index(url, source)` to fetch and index web pages
- `ctx_execute(language: "javascript", code: "const r = await fetch(...)")` to run HTTP calls in sandbox

### Inline HTTP — BLOCKED
Any Bash command containing `fetch('http`, `requests.get(`, `requests.post(`, `http.get(`, or `http.request(` is intercepted and replaced with an error message. Do NOT retry with Bash.
Instead use:
- `ctx_execute(language, code)` to run HTTP calls in sandbox — only stdout enters context

### WebFetch — BLOCKED
WebFetch calls are denied entirely. The URL is extracted and you are told to use `ctx_fetch_and_index` instead.
Instead use:
- `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` to query the indexed content

## REDIRECTED tools — use sandbox equivalents

### Bash (>20 lines output)
Bash is ONLY for: `git`, `mkdir`, `rm`, `mv`, `cd`, `ls`, `npm install`, `pip install`, and other short-output commands.
For everything else, use:
- `ctx_batch_execute(commands, queries)` — run multiple commands + search in ONE call
- `ctx_execute(language: "shell", code: "...")` — run in sandbox, only stdout enters context

### Read (for analysis)
If you are reading a file to **Edit** it → Read is correct (Edit needs content in context).
If you are reading to **analyze, explore, or summarize** → use `ctx_execute_file(path, language, code)` instead. Only your printed summary enters context. The raw file content stays in the sandbox.

### Grep (large results)
Grep results can flood context. Use `ctx_execute(language: "shell", code: "grep ...")` to run searches in sandbox. Only your printed summary enters context.

## Tool selection hierarchy

1. **GATHER**: `ctx_batch_execute(commands, queries)` — Primary tool. Runs all commands, auto-indexes output, returns search results. ONE call replaces 30+ individual calls.
2. **FOLLOW-UP**: `ctx_search(queries: ["q1", "q2", ...])` — Query indexed content. Pass ALL questions as array in ONE call.
3. **PROCESSING**: `ctx_execute(language, code)` | `ctx_execute_file(path, language, code)` — Sandbox execution. Only stdout enters context.
4. **WEB**: `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` — Fetch, chunk, index, query. Raw HTML never enters context.
5. **INDEX**: `ctx_index(content, source)` — Store content in FTS5 knowledge base for later search.

## Subagent routing

When spawning subagents (Agent/Task tool), the routing block is automatically injected into their prompt. Bash-type subagents are upgraded to general-purpose so they have access to MCP tools. You do NOT need to manually instruct subagents about context-mode.

## Output constraints

- Keep responses under 500 words.
- Write artifacts (code, configs, PRDs) to FILES — never return them as inline text. Return only: file path + 1-line description.
- When indexing content, use descriptive source labels so others can `ctx_search(source: "label")` later.

## ctx commands

| Command | Action |
|---------|--------|
| `ctx stats` | Call the `ctx_stats` MCP tool and display the full output verbatim |
| `ctx doctor` | Call the `ctx_doctor` MCP tool, run the returned shell command, display as checklist |
| `ctx upgrade` | Call the `ctx_upgrade` MCP tool, run the returned shell command, display as checklist |
