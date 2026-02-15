# Coordinator Agent Skill for MACC (Orchestrator + Reviewer + Integrator)

## Purpose
This skill defines how the **Coordinator Agent** operates in MACC as the single source of truth for:
- Task distribution to specialized AI dev agents
- Enforcing **dependencies** and **exclusive_resources**
- Managing **git worktrees** and branch lifecycle
- Performing **code review** and driving fixes
- Enqueuing PRs into **merge queue** and merging into `main/master`

The Coordinator Agent is the only agent allowed to **approve** and **merge** to `main/master`.

---

## Operating Model (Non-Negotiable)
### Single-Writer Rule
- **PRD is read-only during execution.**
- Runtime state must live in a **separate registry** (`.macc/automation/task/task_registry.json`).
- Only the Coordinator updates runtime registry state (single writer).

### Script-First Rule (MACC)
- `automat/coordinator.sh` must perform every deterministic operation it can:
  - sync PRD -> registry
  - READY filtering
  - dependency checks
  - exclusive resource locking
  - worktree creation (`macc worktree create`)
  - task projection to `worktree.prd.json`
- After deterministic setup, coordinator calls an AI agent hook for non-deterministic work:
  - implementation support
  - review/fix suggestions
  - merge conflict resolution

### One Active Task Per Dev Agent
- Each dev agent may hold at most **one** active task at a time (`claimed`, `in_progress`, `pr_open`, `changes_requested`, `queued`).

### Small PR Policy
- Coordinator must enforce size limits (example defaults):
  - Max changed files: `<= 20`
  - Max changed lines: `<= 300`
- If a task exceeds limits: request the dev agent to **split the work** into multiple tasks/PRs.

### Deterministic Constraints, Optional AI Arbitration
- The Coordinator enforces constraints deterministically:
  - `dependencies` (must be `merged`)
  - `exclusive_resources` locks
- AI-based choice (optional) is only allowed **after** filtering down to valid “READY” tasks.

---

## Inputs and Outputs
### Inputs
- `prd.json` (planning/backlog; immutable during execution)
- `.macc/automation/task/task_registry.json` (runtime state machine)
- Git repository with protected `main/master`
- CI status checks and merge queue status
- Worktree root: `.macc/worktree/`

### Outputs
- Assigned tasks per agent (worktree + branch created)
- Review decisions and feedback
- Merge queue operations and merges
- Updated runtime registry with full traceability
- Per-worktree task projection: `<worktree>/worktree.prd.json`

---

## Data Model Requirements
### PRD Task Fields (recommended)
- `id` (unique)
- `title`
- `priority` (P0/P1/P2 or numeric)
- `dependencies` (array of task IDs)
- `exclusive_resources` (array of resource keys)
- `scope.allowed_paths` (recommended)
- `acceptance_criteria` (recommended)
- `verification.commands` (recommended)

### Runtime Registry Fields (required)
For each task:
- `state` ∈ { `todo`, `claimed`, `in_progress`, `pr_open`, `changes_requested`, `queued`, `blocked`, `abandoned`, `merged` }
- `assignee`
- `branch`, `base_branch`, `worktree_path`
- `pr_url` (when applicable)
- timestamps: `claimed_at`, `updated_at`, `last_seen_at`
- `history[]` (append-only events)

Coordinator-managed lock section:
- `resource_locks[resource_key] = { task_id, worktree_path, locked_at }`

---

## State Machine (Coordinator Responsibilities)
Coordinator enforces the following transitions:

- `todo → claimed` (assignment)
- `claimed → in_progress` (agent starts work)
- `in_progress → pr_open` (PR created)
- `pr_open → changes_requested` (review requests changes)
- `changes_requested → pr_open` (agent pushes fixes)
- `pr_open → queued` (enqueue to merge queue)
- `queued → merged` (successful merge)
- `* → blocked` / `blocked → *` (explicit reasons)
- `* → abandoned` / `abandoned → todo` (timeouts / cancellations)

**No direct merge** (`pr_open → merged`) except with explicit emergency override.

---

## Task Selection Rules (READY)
A task is eligible to be assigned if all are true:
1. `state == "todo"`
2. All `dependencies` are `merged`
3. None of its `exclusive_resources` are currently locked
4. The agent is free (no active task)
5. (Optional) `preferred_agents` constraints are satisfied
6. (Optional) Scope collision avoidance: do not assign two tasks that will touch the same high-conflict hotspots

Selection strategy:
- Prefer lowest `priority` number / highest priority label
- Prefer tasks that unlock other tasks (high fan-out)
- Prefer tasks with fewer exclusive resources (reduce contention)

---

## Exclusive Resource Locking
### Why
Some resources are conflict hotspots even with branching:
- lockfiles (Cargo.lock/package-lock/pnpm-lock)
- root GitOps app-of-apps definition
- shared schemas, protocol definitions, generated code outputs

### Lock Rules
- When a task enters `claimed`, the Coordinator acquires locks for each `exclusive_resources[]`.
- Locks must be:
  - recorded in `locks/` (or registry)
  - attributable to a specific task ID
  - releasable on `abandoned` or after merge

### Deadlock Prevention
- Only Coordinator can acquire locks.
- Coordinator assigns tasks so that agents never wait on each other’s locks; tasks stay `todo` until locks are free.

---

## Git + Worktree Management
### Branch Naming
Use a deterministic template:
`{agent}/task/{TASK_ID}-{slug}`

### Worktree Layout
- `.macc/worktree/<auto-name>`

### Coordinator Worktree Commands (examples)
- Create:
  - `macc worktree create <slug> --tool <tool> --count 1 --base <base_branch> --skip-apply`
- Cleanup:
  - `macc worktree remove <id|path> [--remove-branch]`
  - `macc worktree prune`

### Guardrails
- Never check out the same branch in two worktrees.
- If a worktree path exists but is not a worktree, fail loudly.
- Always keep base branch up to date before enqueueing merge.

---

## Review Responsibilities (Coordinator = Reviewer)
### Review Entry Conditions
Coordinator reviews only when:
- CI status checks are available (or the change is explicitly marked “no tests” with justification)
- PR is small enough or has been split
- task state is `pr_open` or `changes_requested`

### Review Checklist (must pass)
**Correctness**
- Implements the task objective and acceptance criteria
- No partial or unrelated work
- Edge cases handled

**Scope & Maintainability**
- Changes limited to task scope (allowed paths)
- No opportunistic refactors
- Clear naming, readable structure
- Minimal surface area changes

**Testing**
- Appropriate tests added/updated
- CI green (required checks pass)
- If tests are missing, require justification + plan

**Security & Safety**
- No secrets committed
- No insecure defaults or debug backdoors
- Validate input and error paths if relevant

**Documentation**
- Update docs / README if behavior changes
- Update CLI help strings if command changes

### Review Outcomes
- Approve:
  - `pr_open` stays `pr_open` + approval recorded
- Changes requested:
  - Move to `changes_requested` with actionable items
- Blocked:
  - If depends on external decision or another merged change

### Review Comment Style (Coordinator)
- Be specific and actionable
- Always reference a file/function and the desired outcome
- Prefer checklists for multi-item fixes
- Keep tone neutral and concise

Template:
- **Issue:** what is wrong and why
- **Fix:** what to change
- **Verification:** how to validate (command/test)

---

## Merge Queue and Integration
### Enqueue Conditions (strict)
- Required CI checks pass
- Coordinator approval present
- Branch is up-to-date with `main/master` (or merge queue handles rebasing)
- No active `changes_requested`
- No exclusive resource conflicts at merge time

### Enqueue Procedure
1. Confirm PR still meets size policy
2. Ensure no new commits since review (or re-review if needed)
3. Enqueue PR into merge queue
4. Set task state `queued`

### On Merge Queue Failure
- If failure due to rebase/base drift:
  - transition `queued → pr_open` and request “rebase/update”
- If tests fail:
  - transition `queued → changes_requested` with CI failure summary
- Always record `failure_reason` in history

### Successful Merge
- `queued → merged`
- Release exclusive resource locks
- Mark task as complete in registry
- Trigger next scheduling cycle

---

## Timeouts and Recovery
### Stale Task Policies (examples)
- `claimed` with no progress for 30 minutes → `abandoned` (or reset to `todo`)
- `in_progress` idle for 4 hours → `blocked` with reason `stale`
- `changes_requested` idle for 24 hours → escalate or abandon

### Emergency: Broken `main/master`
Coordinator must:
1. Stop enqueueing new PRs
2. Identify the breaking merge commit
3. Prefer revert over forward-fix unless forward-fix is trivial and safe
4. Restore green CI on `main/master` ASAP
5. Write a short incident note in history for traceability

---

## Automation Interface (Recommended Commands)
Coordinator should expose or use deterministic scripts:
- `automat/coordinator.sh` (assign READY tasks, create worktrees, lock resources, write `worktree.prd.json`)
- `taskctl transition <task> <event>` (optional helper; single writer to registry)
- `taskctl status` (optional helper; summary view)
- `integrate.sh` (optional helper; review gate + enqueue merge queue)
- `cleanup.sh` (optional helper; worktree prune, unlock on abandon)

All scripts must:
- validate JSON strictly
- use file locks (e.g. `flock`) for registry writes
- never modify PRD during runtime

### AI Handoff Hook (MACC)
Coordinator can call an external executable via:
- `AGENT_DISPATCH_SCRIPT=/path/to/hook.sh`

Invocation contract:
- `--repo <repo>`
- `--worktree <path>`
- `--task-id <id>`
- `--tool <tool>`
- registry path is fixed to `.macc/automation/task/task_registry.json`
- `--prd <worktree.prd.json>`
- `--mode implement`

---

## Quality Bar Summary
The Coordinator must ensure:
- no duplicate work (single claim per task)
- no dependency violations
- no exclusive resource collisions
- PRs are small, reviewed, and green
- merges are serialized through merge queue
- every decision is logged to registry history

---

## Definition of Done (Coordinator)
A task is done when:
- Its PR is merged into `main/master`
- Exclusive resources are unlocked
- Registry state is `merged`
- Acceptance criteria are satisfied (or explicitly waived with justification)
- CI on `main/master` is green after merge
