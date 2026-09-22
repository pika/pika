# Design working areas

One subdirectory per subject. These are planning documents: nothing here is built into the documentation site, imported, or shipped. `connection-recovery/` is the current subject; start at its `README.md`.

Nothing here is checked mechanically. A checker for the rules below was built and then removed: it reached two and a half times the size of the documents it was checking, and its most valuable part - verifying that every backticked `Class.member` exists on the real class - stops being trustworthy once 2.0 begins moving the API these documents cite, because a mismatch then means the code has not caught up rather than that the document is wrong. Apply the rules by reading.

## Hard rules for every document under `design/`

These apply to every subject area, present and future. They are rules rather than preferences because each was adopted after the absence of it cost real work.

**A document states the design. It never narrates its own revision history.** No `an earlier draft said`, no `this was previously specified as`, no `that claim is retracted` - and note that a banned phrase can be discussed, as here, by writing it as code, which is also the right typography for a literal specimen. A rule is justified by the engineering reason it holds, never by what a past version of the document got wrong. The test is whether the document reads as though it were correct from the first line, because that is how an implementer reads it: a specification that explains itself through its own mistakes reads as a pile of errata, and the reader cannot tell which sentence is in force. Where a rejected alternative is genuinely worth recording - because it is the obvious design from a standing start and someone will propose it again - it goes in a document that declares itself history in its opening lines, not in the specification.

**Where a document has open questions, they are its first section.** Those are the decisions a human reader owes an answer to, so they go where that reader lands rather than past the mechanism. Each entry says what is already decided and what remains, so a settled choice is distinguishable from a pending one without reading further.

**One canonical location per fact.** Restating a mechanism in two places guarantees the copies drift; cross-reference instead. This extends across documents: no heading may appear in two documents of the same subject area, because pointers resolve against headings pooled across that subject, and a duplicate makes every pointer to it ambiguous. A subject is a top-level directory here, so a document filed one level deeper still resolves against, and must not collide with, its subject.

**Never hard-wrap.** Every paragraph and list item is one line, however long. The renderer reflows it anyway, and wrapping breaks line-anchored citation, `grep -n`, and per-line diff review.

**Cite the symbol, not the prose description.** A claim about current behaviour names the symbol it was checked against, so a reader can re-verify it. `file:line` is for the narrower case where a specific line is the evidence; a line number in a moving `main` goes stale in silence.

**A manifest names and points; it does not explain.** Sections that list files, symbols or phases exist so an implementer can work from them. Mechanism belongs in the prose, with the manifest pointing at it, so a correction lands in one place instead of going stale in the other.
