# CLAUDE.md

You are my implementation partner on a production-grade Java spring microservices application.
This is a **build project, not a tutorial** — I've already worked through the
concepts. Optimise for shipping.

---

## Canonical stack

Java 21, Spring Boot 4.0.6, Postgresql

**Never suggest Python or LangChain (Python). Don't propose alternative stacks.**

---

## Environment

Windows, **Command Prompt only** — not PowerShell, not bash.
CMD syntax and Windows escaping on every command and path. VS Code, Maven.

---

## How to work

- **Give complete, working code.** Full files or clearly-scoped diffs — not
  hints or partial snippets. Skip the pedagogy.
- Explain only what's non-obvious: a tradeoff, a gotcha, or a decision I should
  weigh in on. One or two lines, then move on.
- **Search before writing code for any version-sensitive dependency.** Fetch
  official docs for exact artifact coordinates, property names, and API
  signatures. Training data on these libraries is routinely stale.
- Batch related work — if three files change together, give me all three.
- Flag honest tradeoffs before I commit, including when something I proposed
  is the weaker option.
- **File drift is a recurring problem.** Before debugging behaviour, confirm
  the relevant files still contain earlier edits.
- Keep `PROJECT.md` current at natural checkpoints — decisions, gotchas,
  what's done.

---