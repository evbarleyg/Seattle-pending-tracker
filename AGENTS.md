# Codex instructions (Build mode default)

## Default mode: Build mode
- Optimize for progress on a hobby web app, but keep code clean and explicit.
- You may make code changes without asking first unless the change is large, irreversible, or architectural:
  - new major dependency/framework choice
  - DB schema/data model change
  - auth/session/security boundary change
  - large refactor that changes public interfaces
  - or > ~100 LOC across multiple files
- Be DRY when it improves clarity; avoid premature abstraction.
- Tests:
  - Required for: utilities/business logic, bug fixes, tricky edge cases.
  - Optional for: UI-only iteration and cosmetic changes.
- After any code changes, include:
  - a short changelog (bullets)
  - risks/edge cases to watch
  - suggested follow-ups (hardening tasks)

## Review mode (opt-in)
If I say “Switch to Review mode”:
- Do not change code until I explicitly approve a plan.
- Do a structured review: Architecture, Code Quality, Tests, Performance.
- For each issue: include file/line refs, 2–3 options (including do nothing), and for each option: effort, risk, impact, maintenance burden.
- Give an opinionated recommendation mapped to my preferences, then ask me to choose before proceeding.
- After each section, pause and ask for feedback before moving on.

## Context hygiene (for Codex)
- Keep AGENTS content focused on coding workflow, repo conventions, and execution quality.
- Exclude identity/contact/hobby/life-planning context unless the repository domain explicitly requires it.
- Prefer repo-local facts over generic biography when deciding implementation behavior.

## Build workflow defaults
- For non-trivial tasks, start with explicit assumptions and a short plan.
- For major decisions, provide 2-4 options with tradeoffs and an opinionated recommendation.
- Keep changes small and reviewable unless I explicitly request a broad refactor.
- Do not introduce major new dependencies without justification and alternatives.

## Output contract for code changes
- Include exact commands to validate the change (tests, lint, typecheck) or state clearly when not configured.
- Include concise manual verification steps.
- Include rollback or migration notes when applicable.

## Debugging defaults
- Start with the top 2-3 most likely causes.
- Propose the most likely fix path first.
- If needed, ask for the single most diagnostic datapoint next.

## Repo command defaults
- Test command: `node --check scripts/*.js`
- Lint command: `Not configured in this repo`
- Typecheck command: `Not configured in this repo (JavaScript-only scripts, no TypeScript config)`
- Build command: `node scripts/build_public_proxy_csv.js && node scripts/build_mls_enriched_dataset.js`
- Run/dev command: `node scripts/serve.js`
