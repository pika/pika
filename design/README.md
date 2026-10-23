# Design working areas

One subdirectory per subject. These are planning documents: nothing here is built into the documentation site, imported, or shipped. `connection-recovery/` is the current subject; start at its `README.md`.

Nothing here is checked mechanically. Apply the rules below by reading.

## Rules for every document under `design/`

- Describe behaviour, not code. A design says what an application observes, what must hold, and how success is shown. It names code only where it is public API, or where an invariant cannot be stated without it. A design written against today's private names goes stale as soon as the implementation starts, and it guides an implementer toward the code that exists rather than the outcome that is wanted.
- A document states the design. It never narrates its own revision history. No `an earlier draft said`, no `this was previously specified as`. A rule is justified by the reason it holds, never by what a past version got wrong, so the document reads as correct from its first line. A rejected design worth recording, because someone would otherwise propose it again, goes in the subject's list of rejected designs with the reason it fails.
- Open questions come first in the document that keeps them. Each says what is already settled around it and what remains, so a reader can tell a pending choice from a made one.
- One canonical location per fact. Restating a rule in two places lets the copies drift. Give decisions, invariants and scenarios identifiers, and refer to them by identifier.
- Say where the design diverges from the reference clients, and why. A reviewer who knows the Java, .NET or Go client will otherwise assume a divergence is an oversight.
- Never hard-wrap. Every paragraph and list item is one line, however long. Wrapping breaks line-anchored citation, `grep -n` and per-line diff review.
