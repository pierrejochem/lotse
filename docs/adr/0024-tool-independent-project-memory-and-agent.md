# 24. Tool-independent project memory and agent

- Status: Accepted
- Date: 2026-10-27

## Context

The project is developed with AI coding agents. Instructions kept in one tool's private memory are lost when the tool changes and invisible to other contributors.

## Decision

Project memory lives in the repository, in tool-neutral files: [`AGENTS.md`](../../AGENTS.md) holds the rules, commands and conventions every contributor and agent follows; the dedicated coding agent is described in plain Markdown in [`agents/lotse-engineer.md`](../../agents/lotse-engineer.md). Tool-specific files (`CLAUDE.md`, `.claude/agents/`) only point to these. Decisions go to ADRs, not to agent memory. Commits carry the human author only, without AI co-author trailers.

## Consequences
- Any agent or person gets the same context from a checkout.
- Changing tools means adding a pointer, not rewriting the knowledge.
