---
name: multiagent
description: Multi-agent workflow for an engineering task (plan, implement, test, review). Use when the user asks to build or change something non-trivial and wants it run through the planner/implementer/tester/reviewer pipeline.
argument-hint: <task description or ticket link>
disable-model-invocation: true
---

Run the engineering task through a multi-agent pipeline. You are the orchestrator: you do not write
code yourself, you delegate to subagents and hand each of them only the context it needs.

Task: $ARGUMENTS

Subagents live in `.claude/agents/`: `planner`, `implementer`, `tester`, `reviewer`.
Every agent must follow `AGENTS.md` and `docs/engineering-principles.md`.

## Stages

1. **Plan.** Call `planner` with the task.
   - If it returns `STATUS: NEEDS_CLARIFICATION`, resolve the open questions from its
     "Uncertainties" section. Answer factual questions yourself by reading the code or docs; ask the
     user (AskUserQuestion) only about decisions, one batch at a time. Then call `planner` again with
     the original task plus the decisions made. Repeat until `STATUS: READY`.
   - Show the user the plan briefly. If the plan has a BLOCKER, or the task touches production,
     migrations, personal data or real money movement, stop and ask the user. Otherwise continue.
2. **Branch.** Create a feature branch in every affected repository, never work on `main`.
   Name: `feat/<short-name>` or `fix/<short-name>`. If the directory is not a git repository, tell the
   user and skip this stage.
3. **Implement.** Call `implementer` per plan step. Run independent steps (different files) in
   parallel, in one message with several calls; run dependent steps in order. Give each one: its step,
   the file list, acceptance criteria, and the path to `AGENTS.md`.
4. **Test.** Call `tester`. If RED, send the concrete failures back to `implementer` and re-run the
   tests.
5. **Review.** Call `reviewer`. It judges independently: give it only the plan and the repository
   path, never a retelling of how development went. On CHANGES REQUESTED, send the BLOCKER and MAJOR
   findings to `implementer`, then repeat tests and review.
6. **Iteration limit.** At most 2 cycles of "fix, test, review". If it still does not converge, stop
   and show the user what is in the way.
7. **Report.** Summarize: what was done, changed files, test result, review verdict, remaining MINOR
   findings. Do not commit or open a PR unless the user explicitly asks. When asked: English PR
   title, empty description.

## Orchestrator rules

- Do not duplicate subagent work or read code yourself, except to resolve a conflict.
- Pass short structured results between stages, not whole outputs.
- No deploys and no actions on production.
