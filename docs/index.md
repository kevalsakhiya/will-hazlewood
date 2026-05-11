# broker_scout — documentation index

Five docs cover the system at the level where reality changes slowly. For *living* concerns (current work, individual tickets, conventions of-the-week) keep using the existing top-level files:

| File | What it's for |
|---|---|
| [`README.md`](../README.md) | First-time setup. Operator reads this to get a working dev box. |
| [`plan.md`](../plan.md) | Architecture decisions and rationale. The "why we built it this way" history. |
| [`roadmap.md`](../roadmap.md) | Phased build plan with checkboxes. The "what's next" tracker. |
| [`RULES.md`](../RULES.md) | Coding conventions. New code must match. |

The `docs/` folder picks up where those leave off — long-lived reference material that doesn't need to change every sprint.

## Pick a doc

| If you want to… | Read |
|---|---|
| Get a plain-English walkthrough of the whole system | [`workflow.md`](workflow.md) |
| Understand how the pieces fit together | [`architecture.md`](architecture.md) |
| Know what shape an item is at each layer | [`data-flow.md`](data-flow.md) |
| Know what every emitted stat means | [`data-flow.md`](data-flow.md#stat-namespaces) |
| Look up a specific table or column | [`database.md`](database.md) |
| Run / debug / triage a spider run | [`operators.md`](operators.md) |
| Know what an alert means + how to react | [`operators.md`](operators.md#alerts--what-they-mean) |
| Look up a specific monitor's threshold + intent | [`monitors.md`](monitors.md) |
| Add a new platform spider | [`RULES.md` §19](../RULES.md) → [`architecture.md`](architecture.md#extending-with-a-new-platform) |
| Add a new pipeline / column / stat / migration | [`RULES.md` §13–§16](../RULES.md) → [`data-flow.md`](data-flow.md) |

## What's NOT here, and why

- **Per-file API references.** Module docstrings + type annotations are the source of truth (RULES §17.2 / §17.3). A doc that says "`brokers_repo.insert_brokers(items, run_id, scrape_date)` inserts brokers" rots the moment the signature changes; reading the function does not.
- **Setup walkthroughs.** [`README.md`](../README.md) already has the OAuth bootstrap, Sheets template setup, and `.env` walkthrough. Duplicating drifts.
- **Phase-by-phase narrative.** [`roadmap.md`](../roadmap.md) is the source of truth for what shipped when.
- **Coding rules.** [`RULES.md`](../RULES.md) is the source of truth. These docs reference it, never restate it.

## Updating these docs

If reality diverges from a doc here, *fix the doc in the same commit that diverges from it.* Same rule as RULES.md (§22). A wrong doc is worse than no doc — operators trust them.

The moment any per-file content creeps in (e.g. "`extract_basic` takes these arguments…"), delete it and rely on the docstring + signature. These docs are about contracts, flows, and operator playbooks — never internal function shapes.
