# AGENTS.md — Autonomous Agent Operational Guidelines

This file governs the operational behavior, execution loops, safety boundaries, and tool discovery mechanisms for autonomous agents operating within this repository.

---

## 1. Core Operating Philosophy
- **Local-First & Terminal-First:** All operations must execute through local shell commands, scripts, and binaries. Avoid high-level abstraction frameworks, heavy third-party automation tools, or Node/npm dependencies.
- **Raw Execution:** Prefer direct API calls using `curl`, standard Unix utilities, and lightweight binaries over opaque wrapper scripts.
- **Autonomous Error Recovery:** If a command or build fails, read the error output, inspect state, patch the code or script locally, and retry autonomously.
- **Use the GH cli** This is your entrypoint for your version control. Use it. If you are working on a issue, add comments to it as needed.
---

## 2. Dynamic Tool Discovery
Agents must not assume static tool availability. Before invoking custom tools or local harnesses, agents must discover available capabilities dynamically:
1. Scan local runner directories or configuration paths for executable scripts.
2. Execute discovered tools with the `--schema` flag to inspect accepted inputs, parameters, and expected output formats (e.g., `./bin/tool --schema`).
3. Parse the returned JSON/text schema before invoking the tool in production flows.

---

## 3. Autonomous Execution Loop
When assigned a task or processing a queue item, the agent must follow this strict loop:

1. **Intake & Scope:** Read instructions or task items from the local queue directory or prompt context.
2. **Discovery:** Check environment variables, available binaries, and local schemas.
3. **Execution:** Run the implementation or generation step via shell execution.
4. **Validation:** Automatically run local validation suites, tests, or linters (e.g., `make test`, `make lint`) to verify correctness.
5. **Reporting:** Log execution summaries, errors, or completion states to the local tracking mechanism (e.g., local log files or queue status updates).

---

## 4. File & Artifact Management
- **Edits:** Modify files directly using local text tools or clean shell redirection. 
- **Creation:** When generating code, documentation, or configuration files, write directly to the appropriate repository path.
- **Cleanup:** Ensure temporary files, scratch buffers, and intermediate test artifacts are cleaned up post-execution.

---

## 5. Constraints & Guardrails
- **No Docker / No WSL:** Do not attempt to spin up Docker containers, docker-compose stacks, or Windows Subsystem for Linux environments. Operate directly on the host OS environment.
- **Resource Discipline:** Avoid memory-heavy processes or high-overhead runtime packages. Keep resource consumption minimal.
- **Strict Boundaries:** Do not execute destructive system commands (e.g., `rm -rf /`, unauthorized network scans, or credential leaks). Operate strictly within repository boundaries and designated local paths.
- **Novel Branches:** If you plan to edit **ANY** file, make a new branch before you do. At the end of the work, commit your changes and do a pr to the main branch.

## 6. Design Rules

These are expectations the review history proved, not style preferences. Each rule names where it applies and why it exists. A PR that breaks one should carry a test or a reason.

### 6.1 Race the sweep under the lock

- **Where:** any background sweep that touches state a foreground operation also touches — outbox/relay sweeps, scheduled-post publish passes, backup pruning, pairing-history snapshots.
- **Rule:** the sweep runs under the same lock as the operation it races, or it re-checks its decision after acquiring the lock. Never let a sweep act on state it read before the lock.
- **Why:** the group-retract sweep and a retract marker could race — a leg read before the marker landed would be sent after the retract (#706); the history-retry gate reads its markers under one bounded mailbox transaction for the same reason (#743).

### 6.2 Re-read across processes, never trust a cached copy

- **Where:** anything shared between runs, processes, or machines — the on-disk data dir, the PR head on GitHub, a review comment's footer.
- **Rule:** re-read the live state immediately before acting on it. A check made before the last push, the last run, or the last process restart is stale.
- **Why:** GitHub's PR-head reads lagged pushes for minutes, so merges landed on pre-fix heads repeatedly (#661, #729, #739, #764, #768); timed-out runs left staged reverts of real fixes in the shared clone; Qodo reviews update in place, so only the footer commit plus `updated_at` say which head was reviewed.

### 6.3 Normalize case and ids at the boundary

- **Where:** every place an identifier or name crosses into the system — peer hashes, group names, invite tokens, reaction targets.
- **Rule:** canonicalize at ingestion (one case, one id form) and compare only canonical forms. Never compare raw user input against a stored id, and never use a display name where an id is required.
- **Why:** an invite check compared a token's mint-time name against the live group name exactly and rejected legitimate joins after a rename (#702); a contact card rendered through `display_name` leaked a local alias to another peer (#762).

### 6.4 Quality gate — what "done" means

A task is not done until all of these hold. This is the merge loop's critical-error gate, written down so the ticket loop, the merge loop, and Qodo all enforce the same bar.

- **Qodo findings:** every valid Critical or Major is fixed on the branch; every valid Warning is fixed too (Warnings here are Medium-severity in practice). A Warning that can't be fixed in scope becomes a follow-up issue the same day, never a silent pass. Informational findings are flagged for the human, not silently dropped.
- **CI:** all checks green on the PR's current head — not an older head, not a re-run of a stale commit.
- **Local validation:** `make lint` and `make test` pass on the tree merged with current main; the integration and web suites pass when the change touches them.
- **Review freshness:** the latest completed Qodo review covers the current head — check the review comment's footer commit and its `updated_at`, not just that a review exists. Never merge an unreviewed PR.
- **Disputes need evidence:** a finding left unchanged carries a reason grounded in the code (a test, a quoted invariant, a linked prior decision). "Qodo is wrong" without evidence is not a reason.
- **A stalled loop stops:** if a fix-and-review cycle stops making progress, the task is reported as not done with the blocker named — it is never waved through.

