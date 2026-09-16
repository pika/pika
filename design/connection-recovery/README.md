# Connection recovery design

Working area for pika's built-in connection and topology recovery, tracking the discussion in [pika #1654](https://github.com/pika/pika/discussions/1654). These are planning documents, not user-facing docs; they live outside `docs/` deliberately so they are not built into the published documentation site.

## Constraints for this design (read first)

These are settled facts about the target release, not open questions. They override anything below that assumes otherwise, including text written before they were recorded here.

- **The target is pika 2.0.0.** Breaking changes to the public API are permitted where the design needs them. "This preserves backward compatibility with 1.x" is not a requirement to satisfy, and not a reason to prefer one shape over another.
- **2.0 has exactly one connection type and one channel type.** `Connection` and `Channel`, currently in `pika/adapters/thread_safe_connection.py`, to be relocated by the 2.0 restructure. Every other public adapter - asyncio, blocking, gevent, tornado, twisted - is removed. `SelectConnection` survives only as internal machinery backing `Connection`, not as a public adapter. There is therefore no "which adapters does this apply to" question and no per-adapter recovery driver to design.
- **The classes are already named `Connection` and `Channel`.** That rename shipped in 1.5.0, see #1617. `ThreadSafeConnection` and `ThreadSafeChannel` no longer exist. The new names collide with `pika.connection.Connection` and `pika.channel.Channel`, so always say which one is meant.

Documents:

- `proposal-recovery.md` - the design proposal, and the authoritative document here: recovery as a first-class `RECOVERING` state on the adapter connection and channel handles, driven on the connection's own persistent IOLoop. It started as a verbatim import of the [gist](https://gist.github.com/suchitd/dd6c22163186f19a2ab07569315b6ac1) so it would be versioned and diffable here, and was subsequently rewritten to follow the state-machine framing below.
- `design-state-machine.md` - the framing the proposal now follows, written up while the direction was still being evaluated: recovery as a first-class state on the connection/channel handles, with a dedicated catchable exception when an operation is attempted during recovery, driven on a persistent loop rather than a separate thread. Kept for how the framing was derived, the AMQP 1.0 Java client precedent it borrows from, and the questions it raised.
- `findings.md` - what we measured about how the RabbitMQ Java client and amqp091-go actually behave when publishing during recovery. The harnesses that produced these results are at https://github.com/lukebakken/amqp091-misc.

The documents are living drafts meant for collaborative editing, and nothing in them is settled. The constraints above are the exception: they are settled, and a draft that contradicts one is wrong rather than alternative.
