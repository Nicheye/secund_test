# AGENTS.md

Entry point and map for humans and coding agents working in this repository.
`CLAUDE.md` simply imports this file, so there is one source of truth.

## Project

Backend take-home task for a middle+/senior fintech backend position.
Async payment processing service. Stack: Python, FastAPI + Pydantic v2, SQLAlchemy 2.0 (async),
PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose. Spec: [docs/task-spec.md](docs/task-spec.md).

## Docs

| Doc | What is in it |
| --- | --- |
| [README.md](README.md) | What the service is, architecture, how to run and test it, API examples, design decisions and trade-offs. |
| [docs/engineering-principles.md](docs/engineering-principles.md) | Engineering principles, naming, testing, API and fintech-specific rules. Read before writing or reviewing code. |
| [docs/task-spec.md](docs/task-spec.md) | The assignment: entities, API, outbox, retry/DLQ, stack, deliverables, evaluation criteria. |
| [.claude/skills/multiagent/SKILL.md](.claude/skills/multiagent/SKILL.md) | `/multiagent <task>`: plan, implement, test, review pipeline. Subagents are in `.claude/agents/` (`planner`, `implementer`, `tester`, `reviewer`). |

Add new docs to this table. Keep this file a map only: no rules or explanations here.

## Working agreements

- Where a doc and this file disagree, this file wins.
- Do not commit unrelated changes; keep each change to one concern.
- Do not add docs or files that are not linked from the table above.
