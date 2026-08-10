# Agent Instructions

## STOP — Read Beads, Then Create a Bead

Before reading code, editing files, or exploring the codebase for ANY code task:

1. **Read existing beads** for context on past work:
   ```bash
   bd list --status closed
   bd ready
   ```
2. **Create a bead** for the current task:
   ```bash
   bd create "Brief title" --description "Why this exists and what needs to be done"
   ```
   The first positional arg is the **title**, not the repo. Repo is auto-routed from `.beads/`. Don't pass `enhancedchannelmanager` as the title — that's the most common foot-gun.

No exceptions. No "I'll do it later." The bead comes before the first Read, Grep, or Edit.
After the work is deployed and verified, close it: `bd close <bead-id>`

## Beads Quick Reference

```bash
bd ready                      # Find available work
bd show <id>                  # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>                 # Complete work
bd list --status closed       # View closed beads for context
bd sync                       # Sync beads data only (NOT for code commits)
```

- Beads auto-route to this repo via `.beads/`. If you ever need to override (e.g., creating a bead targeting a different rig), use `--repo enhancedchannelmanager`.
- **NEVER chain `bd create` and `bd close`** — run them as separate commands
- The `.git/beads-worktrees/dev` worktree is **only for beads issue tracking** (sparse checkout of `.beads/` only — no code files). Do NOT edit code there.

## Invoking Personas (project-engineer, qa-engineer, sre, etc.)

Personas are skills at `~/.claude/skills/<persona>/SKILL.md`, NOT subagent types. To spawn them — especially in parallel — use the Agent tool with `subagent_type: "general-purpose"` and load the persona identity in the prompt:

```
Read ~/.claude/skills/<persona>/SKILL.md for your domain scope.
Read ~/.claude/skills/<persona>/identity.md (if present).
Read ~/.claude/skills/_shared/engineering-discipline.md.

You are the <Persona>. <question/task>
```

The canonical pattern is documented in `~/.claude/skills/spike/SKILL.md` (Step 3). Do NOT try `subagent_type: "project-engineer"` — the registered subagent types are only `general-purpose`, `Explore`, `Plan`, `claude-code-guide`, `statusline-setup`.

For multi-persona workflows (team-plan, team-review, spike, grooming, standup, retro, onboard), invoke the orchestrating skill via the Skill tool — it handles the fan-out itself.

## Worktree & Agent Isolation (this environment)

The Claude Code worktree mechanism is unreliable here. Two harness-level bugs — NOT fixable in-repo (see bd-j8osn):

- **cwd-trap:** a worktree-spawned agent's Bash cwd resets to the MAIN checkout between calls, while Read/Edit act on the literal absolute path given. An agent that doesn't make EVERY path worktree-absolute ends up editing `dev` directly — silently polluting the main checkout.
- **Lock accumulation:** a finished agent's worktree stays locked and is never auto-cleaned; `git worktree remove` then needs `-f -f`.

Standing defaults (these OVERRIDE the global "worktree-isolate every write agent" rule, which assumes a working mechanism):

1. **Default: non-worktree, sequential engineers.** For typical small / single-domain changes, spawn the engineer WITHOUT `isolation: "worktree"` and brief it to work on a branch in the main checkout (do NOT create a worktree). No second tree ⇒ no cwd-trap, nothing left locked.
2. **Worktrees only for genuinely parallel, independent write agents.** When used: brief the cwd-trap hard (every Read/Edit/Bash path worktree-absolute; verify `git -C <wt> branch --show-current` before any commit), and on return verify each agent's gates AND that the main checkout is clean (`git status` shows none of the agent's files).
3. **Clean up on merge, every time.** `git worktree remove -f -f <path>` is part of the PR-merge step — never defer to a later sweep. Skip and flag any worktree with uncommitted tracked changes instead of force-removing it.
4. **Trust nothing unverified.** Independently re-run the agent's claimed gates before merging — the agent's report is not the gate.

Frontend tooling in a worktree (writable `.vite-temp`) is fixed in `scripts/worktree-bootstrap.sh`; full caveats in `docs/shipping.md` → "Worktree quirks".

## Sizing Vocabulary — Measured Durations Only, Never Invented Ones

Size work as **Small / Medium / Large / Epic — needs decomposition** (per `~/.claude/skills/grooming/SKILL.md`). Do NOT give the PO calendar estimates in hours/days/weeks/months. Calendar estimates invite commitment theater and are almost always wrong.

The reason they are wrong is worth knowing, because it tells you what IS allowed: the model's sense of
"how long a feature takes" comes from training data measuring **human teams** shipping features, so it
says "three months" about work that finishes in a day. The unit is wrong, not the estimate.

**So there is exactly one way to answer a duration question: quote a measured range, or say you don't
have one.** Completed swarm runs are recorded to `~/.claude/swarm-runs.jsonl`, and the answer comes
from `node ~/.claude/skills/swarm/scripts/predict-run.mjs --size <S|M|L|Epic>`:

- **5+ comparable runs** → report the range of ACTIVE minutes with the sample size, e.g. "7 comparable
  runs: 18 to 74 minutes of active work, median 31, and 4 of the 7 needed a fix round." Report the
  range and the count, never a single number.
- **Fewer than 5** → the size class alone, and say the history is still building. The script refuses
  to give a range there; that refusal is the point.
- **Never** convert either figure into hours, days, weeks or months for a person, and never quote
  wall-clock as "how long it takes" — the first measured run logged 797 minutes wall against 52
  minutes active, because wall-clock counts every minute the run sat waiting on the PO.

Full rules, the recorded fields, and how a run gets recorded: `~/.claude/skills/swarm/reference/run-timing.md`.

Exception — governance cadence rules from ADRs (e.g., ADR-005's monthly-then-quarterly audit cadence) are project-defined constraints and can be quoted verbatim. Do not multiply them out into wall-time estimates. Quote the rule; let the PO do the arithmetic if they want it.

## Reference Guides

| Guide | Location |
|-|-|
| Architecture Diagram | `docs/architecture.md` |
| Auth Middleware | `docs/auth_middleware.md` |
| Backend Architecture | `docs/backend_architecture.md` |
| Database Migrations | `docs/database_migrations.md` |
| Pytest Conventions | `docs/pytest_conventions.md` |
| Project Architecture | `docs/project_architecture.md` |
| Runbooks | `docs/runbooks/` |
| **Style Guide (canonical)** | `docs/style_guide.md` |
| CSS Guidelines | `docs/css_guidelines.md` |
| Beads (Issue Tracking) | `~/.claude/projects/<project-slug>/memory/beads.md` |
| Dispatcharr API | `docs/dispatcharr_api.md` |
| Discord Release Notes | `docs/discord_release_notes.md` |
| Frontend Lint Policy | `docs/frontend_lint.md` |
| Testing Details | `docs/testing.md` |
| Shipping Workflow | `docs/shipping.md` |
| Dummy EPG Template Engine | `docs/template_engine.md` |
| DBAS Import Threat Model | `docs/security/threat_model_dbas_import.md` |
| CodeQL Configuration | `docs/security/codeql-config.md` |
| Normalization (user + dev guide) | `docs/normalization.md` |
| Channel Pipeline Rule Analyzer | `docs/channel_pipeline_rule_analyzer.md` |
| Event Sync (user + dev guide) | `docs/event_sync.md` |
| Versioning Scheme | `docs/versioning.md` |
| API Reference | `docs/api.md` |
| SLOs | `docs/sre/slos.md` |
| User Guide (operator-facing) | `docs/user_guide/` |
| Graphify findings (past traces) | `graphify-out/memory/*.md` |
| Graph audit report | `graphify-out/GRAPH_REPORT.md` |

## Architecture Questions

For codebase-architecture questions (how X connects to Y, what a component's role is, where the hot path runs), the order of precedence is:

1. **`docs/architecture.md`** — the hand-curated system overview + Channel Pipeline internals + MCP + external API contract.
2. **`graphify-out/memory/*.md`** — saved Q&A from past graph traces. Each file is one question + answer. Greppable. Cheap to read.
3. **Rebuild the graph** only if (1) and (2) don't cover it: `/graphify backend frontend docs`. Then query via `graphify query "..."` / `graphify explain "NodeName"` / `graphify path "A" "B"`.

The raw `graph.json` and `cross-repo-graph.json` files are gitignored (large, machine-local paths). Rebuild on demand.

**For coding conventions** (naming, module organization, comments, error
handling, regex, CSS, lint, tests), `docs/style_guide.md` is the canonical
reference. Other guides in this table cover their own subject (CSS shared
classes, lint per-rule patterns, etc.) and are cited from the style guide
where they remain authoritative.

## Development Workflow

**Always work from the `dev` branch.** Container name: `ecm-ecm-1`

### Container-First Development

Edit code locally, deploy to container, iterate. Do NOT commit until told to "ship the fix."

```bash
docker cp <local-file> ecm-ecm-1:/app/<destination-path>
```

**Frontend deploy:**
```bash
scripts/deploy-frontend.sh          # build + clean stale assets + copy, in one step
```
The script bakes in the stale-asset cleanup below so it can't be skipped. Equivalent manual sequence (use `--no-build` to skip the rebuild):
```bash
cd frontend && npm run build
docker exec ecm-ecm-1 sh -c 'rm -rf /app/static/assets/*'
docker cp dist/. ecm-ecm-1:/app/static/
```
Always clean `/app/static/assets/` before copying — `docker cp` only adds files, never removes stale bundles. Set `ECM_CONTAINER` to target a container other than `ecm-ecm-1`.

**Backend deploy** (to `/app/`, NOT `/app/backend/`):
```bash
docker cp backend/main.py ecm-ecm-1:/app/main.py
docker cp backend/routers/. ecm-ecm-1:/app/routers/
docker restart ecm-ecm-1
```

**Coupled backend/frontend changes:** When a frontend build depends on a new or
changed backend route, deploy the backend first and wait for its restart to
complete before deploying the frontend:

```bash
docker cp backend/main.py ecm-ecm-1:/app/main.py
docker cp backend/routers/. ecm-ecm-1:/app/routers/
docker restart ecm-ecm-1
ready=0
for attempt in $(seq 1 30); do
  if docker exec ecm-ecm-1 python -c "import os, urllib.request; port = os.environ.get('ECM_PORT', '6100'); urllib.request.urlopen(f'http://localhost:{port}/api/health/ready', timeout=2)" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "ECM backend did not become ready; frontend was not deployed" >&2
  docker logs --tail 100 ecm-ecm-1 >&2
  exit 1
fi
scripts/deploy-frontend.sh
```

This order prevents the new frontend from calling an API contract that the
running backend does not yet provide. The readiness loop is bounded to 30
attempts with a two-second request timeout; `/api/health/ready` is public, so
the in-container probe needs no credentials. Roll back in reverse dependency order:
restore and deploy the previous frontend build first, then restore the previous
backend files and restart `ecm-ecm-1`. Do not roll back the backend while the
dependent frontend is still live.

**Python packages** use `uv` (not pip): `docker exec ecm-ecm-1 uv pip install <package>`

### Shipping (When User Says "Ship the Fix")

Follow `docs/shipping.md`. The full PR-driven flow (branch from `origin/dev`, push, open PR via `gh pr create --base dev`, wait for the 5 required checks, then `gh pr merge --merge --delete-branch`) lives in `docs/shipping.md` §6 — do not duplicate it here.

**Non-negotiable rules:**
- Work is NOT complete until the PR merges into `dev`
- NEVER stop before the PR is merged — an open PR is not a shipped change
- NEVER say "ready to merge when you are" — YOU must drive the merge once the required checks are green

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
