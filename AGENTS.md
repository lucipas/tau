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
