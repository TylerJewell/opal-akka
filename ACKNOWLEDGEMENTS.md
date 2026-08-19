# Acknowledgements

This project is a port of **[permitio/opal](https://github.com/permitio/opal)**.

## What licence permitio/opal is under, and who holds the copyright

Apache License 2.0, © 2021 Or Weis and Asaf Cohen. Read from `LICENSE` in the project
itself, not from a badge.

## What was copied verbatim

**No source was copied.** Not a file, not a function, not a fixture. There is no Python in
this repository and nothing here was translated line by line.

## What was reproduced exactly, without being copied

Two rules, both established by running the original rather than reading it, and both
reproduced so that the two systems agree on the answers a caller sees:

- **How an address expands.** A name splits on `/` and expands to itself and every name
  above it, with the part before the right-most `:` prepended to every result, and a `:`
  with nothing before it dropped. Measured against
  `opal_server.data.data_update_publisher.DataUpdatePublisher.get_topic_combos` over
  fourteen names.
- **Which members a change reaches.** A change reaches a member when the expanded address
  list and the member's watched names share at least one name. Measured against
  `opal_client.data.updater.DataUpdater._update_policy_data` and confirmed on a running
  two-member fleet.

Both are in `io.akka.opal.domain.Address`, written from the measurements, with the
measurements themselves in the port's own record rather than here.

## Is behaviour derived even where no text was copied?

Yes, and that is the whole point of a port. What a change is, what it is addressed to, who
receives it and what each receiver does with it are all derived from what
permitio/opal does. Where this project decided something the original leaves unsettled, the
decision and its reason are in the `Where it differs from permitio/opal` section of
`README.md`, and the original's own behaviour is stated there plainly and left alone.

## What licence that forces on this project

Apache License 2.0. The original is Apache-2.0, this is a derived work of it in behaviour,
and Apache-2.0 is the licence that travels with that. See `LICENSE`.

## Also used

- [Akka](https://akka.io) — the Akka Java SDK, version 3.6.3, which this runs on.
