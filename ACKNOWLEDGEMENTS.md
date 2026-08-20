# Acknowledgements

This project is a port of **[permitio/opal](https://github.com/permitio/opal)**.

## Licence

`permitio/opal` is **Apache License 2.0**, © 2021 Or Weis and Asaf Cohen — read from its
own `LICENSE` file, not from a badge.

## Was anything copied verbatim?

**No.** No file, no fragment of a file, no fixture and no test corpus. Every Java file
here was written for this port.

## Is behaviour derived even where no text was copied?

**Yes, and that is the whole point of it.** Two things in particular were established by
running `permitio/opal`'s own code and are reproduced here to answer the same way:

- **How a topic addresses part of a fleet**, including expansion to every ancestor of the
  topic and the rule that only the right-most colon marks a scope. Checked case by case in
  `opal-port/probes/probe_01_topic_expansion.py` and
  `opal-port/probes/probe_08_conformance.py`.
- **Whether a given change reaches a given subscriber** — string equality against the
  expanded list, not a prefix test. Same probes.

Everything else in this port is its own design, and the places it deliberately answers
differently are listed in `README.md` under *Where it differs from permitio/opal*.

## What licence that forces on this project

Apache-2.0 permits a derived work under other terms provided the notice above is kept.
Nothing here is a modified copy of an Apache-2.0 file, so no file carries that licence
forward; the attribution is kept anyway because the behaviour is derived.

## Also used

- Akka — `akka-javasdk` 3.6.3
