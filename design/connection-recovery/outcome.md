# Outcome

## What an application sees

An application opens a `Connection`, declares its topology, starts consumers and publishes. Then the network drops, the broker restarts, or an operator closes the connection. With no recovery code of its own:

- The connection reports `RECOVERING` and reconnects with backoff, until it succeeds or the delay policy gives up.
- It redeclares what it recorded and re-subscribes the consumers, on the same channel objects with the same callbacks, and only then reports `OPEN` for the connection and every channel.
- Consumers keep receiving. Deliveries unacknowledged at the drop are redelivered by the broker, and acknowledging one of them afterwards is harmless.
- A call made while recovering fails at once with an error the application can catch.
- In confirm mode, a publish whose confirmation was outstanding is reported as having an unknown outcome, so the application can republish it.

When the broker closes one channel, or cancels one consumer, while the connection stays up, that channel or consumer usually recovers on its own while the rest of the connection carries on. `concepts.md` lists the exceptions.

Recovery is on by default (D-1).

## Goals

- An application that declares, consumes and publishes needs no recovery code to survive a broker restart or a network outage.
- An application that cares can observe every state change, catch the recovering error, and bound or stop retrying through its own delay policy.
- No wait in recovery or close can hang forever (I-1).
- In confirm mode, no publish is lost without the application being told (I-7, D-22).

## Non-goals

- Retrying calls on the application's behalf (D-11, O-7).
- Exactly-once delivery. Republishing an unknown outcome can duplicate a message, so consumers must tolerate duplicates.
- Retrying the first connection attempt (D-9).
