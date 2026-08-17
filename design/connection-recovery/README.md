# Connection recovery design

Working area for pika's built-in connection and topology recovery, tracking the discussion in [pika #1654](https://github.com/pika/pika/discussions/1654). These are planning documents, not user-facing docs; they live outside `docs/` deliberately so they are not built into the published documentation site.

Documents:

- `proposal-recovery.md` - the current design proposal (imported verbatim from the [gist](https://gist.github.com/suchitd/dd6c22163186f19a2ab07569315b6ac1) so it is versioned and diffable here). Recovery as an additive coordinator on a dedicated thread.
- `design-state-machine.md` - an alternative framing: recovery as a first-class state on the connection/channel handles, with a dedicated catchable exception when an operation is attempted during recovery, driven on a persistent loop rather than a separate thread.
- `findings.md` - what we measured about how the RabbitMQ Java client and amqp091-go actually behave when publishing during recovery. The harnesses that produced these results are at https://github.com/lukebakken/amqp091-misc.

These are living drafts meant for collaborative editing; nothing here is settled.
