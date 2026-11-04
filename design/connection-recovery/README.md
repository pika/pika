# Connection recovery design

Automatic connection and topology recovery for `pika.Connection` and its channels, targeting pika 2.0.0 and tracking [discussion #1654](https://github.com/pika/pika/discussions/1654). See `../README.md` for the rules these documents follow.

## Constraints

- The target is pika 2.0.0. The public API may change where the design needs it, and compatibility with 1.x is not a reason to prefer one shape over another.
- 2.0 has one connection type and one channel type, `pika.Connection` and its `Channel`. Every other adapter is removed, so there is no per-adapter recovery to design.
- Supported brokers are RabbitMQ 4.1.x through 4.4.x. Behaviour that differs by version is stated per version, and is detected from what the broker advertises rather than from its version number.

## Reading order

1. `outcome.md` - what an application sees when the broker or the network goes away and comes back, and what is out of scope.
2. `concepts.md` - the vocabulary: states, what is recorded and replayed, the events recovery responds to, and the policies an application controls.
3. `invariants.md` - what must never happen, each with the scenario that would catch it.
4. `success-criteria.md` - the acceptance scenarios. Recovery is done when every one of them passes against a real broker.
5. `process.md` - how the work is phased and verified, each phase ending in a set of passing scenarios.
6. `decisions.md` - the open questions, then the settled decisions with their reasons and the reference-client comparison, then the designs already rejected.
7. `findings.md` - what the Java and Go clients measurably do when an application publishes during recovery.

Identifiers: `SC-` scenarios, `F-` faults, `I-` invariants, `D-` decisions, `O-` open questions, `R-` rejected designs. Numbers are not reused.

## Reference clients

The decisions were checked against the source of the RabbitMQ Java client (AMQP 0.9.1), the RabbitMQ AMQP 1.0 Java client, the .NET client and `amqp091-go`, as of October 2026, and of RabbitMQ 4.1.x through 4.4.x. Read the relevant client before changing a decision; `decisions.md` records where pika diverges and why.
