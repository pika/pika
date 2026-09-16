# Connection recovery design

Working area for pika's built-in connection and topology recovery, tracking the discussion in [pika #1654](https://github.com/pika/pika/discussions/1654). These are planning documents, not user-facing docs; they live outside `docs/` deliberately so they are not built into the published documentation site.

Documents:

- `proposal-recovery.md` - the design proposal, and the authoritative document here: recovery as a first-class `RECOVERING` state on the adapter connection and channel handles, driven on the connection's own persistent IOLoop. It started as a verbatim import of the [gist](https://gist.github.com/suchitd/dd6c22163186f19a2ab07569315b6ac1) so it would be versioned and diffable here, and was subsequently rewritten to follow the state-machine framing below.
- `design-state-machine.md` - the framing the proposal now follows, written up while the direction was still being evaluated: recovery as a first-class state on the connection/channel handles, with a dedicated catchable exception when an operation is attempted during recovery, driven on a persistent loop rather than a separate thread. Kept for how the framing was derived, the AMQP 1.0 Java client precedent it borrows from, and the questions it raised.
- `findings.md` - what we measured about how the RabbitMQ Java client and amqp091-go actually behave when publishing during recovery. The harnesses that produced these results are at https://github.com/lukebakken/amqp091-misc.

These are living drafts meant for collaborative editing; nothing here is settled.
