# opal-akka

Push a change out to every machine that asked for it, and let each one apply the changes in
the order they were made, notice when it has missed one, and ask for exactly what it missed.

A port of [permitio/opal](https://github.com/permitio/opal) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

`permitio/opal` keeps a fleet of permission engines up to date: it watches a repository for
rule changes, takes fact changes over a web address, and pushes both out to every engine
listening. It was rebuilt here to find out how precisely a system has to be written down
before it can be rebuilt on a different stack.

Only the pushing was rebuilt. Where a change comes from, and what it is finally written
into, are somebody else's job here.

Those written specifications live in a separate repository, `akka-specify-harness`, under
`opal-port/`. It is private for now.

---

## permitio/opal → this port

📉 2,237 Python lines → **815 Java lines**<br>
📁 10 files → **18 files**<br>
⚡ 64,676 → **60** nanoseconds, applying one change to one machine<br>
⚡ 394 → **316** nanoseconds, working out who a change is addressed to<br>
🎯 19 answers compared → **19 of 19 agree**<br>
🔀 2 of 6 delivery orders end on the older change → **0 of 6**<br>
🔢 0 ways for a machine to tell it missed a change → **1**<br>
🧪 0 rules broken on purpose to check a test notices → **10**

The 64,676 and the 60 are the two systems' own per-change work with no network in the way,
and they are not doing the same amount of it. Read
[`bench/REPORT.md`](bench/REPORT.md) §2 before quoting that pair anywhere.

Full method, and the numbers that did not make this list: [`bench/REPORT.md`](bench/REPORT.md).

---

## What it took to build

⏱️ **4.2 hours** from the first command to the published repository, **2.8** of them active<br>
💬 **739** exchanges with the model<br>
✍️ **765,955** tokens written by the model, **188,988,969** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **45** tests

```bash
python toolkit/tokens.py --port opal
```

The record of every question, and where the time went, lives with the specifications.

---

## What it does

- **Every change to one place gets a number, one at a time, with no gaps.** Two changes to
  the same place can never be applied in the wrong order, whichever arrives first.
- **A machine applies the next number and nothing else.** A change it has already applied is
  recognised and dropped; a change further ahead than the next one is refused and written
  down as missing.
- **What is missing is asked for by number.** A machine that fell behind gets exactly the
  span it missed, in order, instead of re-reading everything.
- **A change is written down before it is sent.** A machine that was switched off while it
  happened can still get it when it comes back.
- **A change reaches everyone watching any part of the path it names.** A change to
  `users/keys` reaches somebody watching `users`.
- **How far behind each machine is can be asked as a question.** For any place, which
  machines are at which number, and whether they have all caught up.

---

## Design decisions

**Sequence number.** Somebody has to decide what order two changes to the same place
happened in, and the only participant that sees both is the place itself. So the place hands
out a number to every change as it arrives, and a machine that applies them in number order
cannot get two of them the wrong way round.

**Cursor.** A machine that is only told "here is a new value" cannot tell a new one from an
old one arriving late. So each machine remembers the number it last applied, which turns
"was this already done?" and "did I miss something?" into two comparisons it can make on its
own.

**Written down before sent.** Keeping unsent changes in memory means losing them if the
memory fills up or the program stops. So a change is written down where it belongs before
anyone is told about it, and anyone who missed it can ask for it afterwards however long
they were away.

**One place at a time.** Ordering everything a publisher sent, across several different
places at once, is what lets a later publish overtake an earlier one at a place they share.
So changes to different places are kept independent, and only changes to the same place are
ordered against each other.

**Numbers, not a live feed, for delivery.** A live feed starts wherever the listener joined
and cannot say what came before. So delivery goes through the numbered path and the live
feed is only for watching, which means somebody watching can still tell what they saw by
reading the numbers.

---

## Running it — the short path

You do not need Java, Maven, or the Akka command-line tool installed. Akka Specify installs
them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/opal-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9010.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Build and test

```bash
mvn verify
```

`mvn test` runs the 30 tests that need no runtime; `mvn verify` adds the 15 that start one.

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9010**.

### Use it

Tell it who is listening, and to what:

```bash
curl -X POST http://localhost:9010/propagation/subscribers/pdp-1 \
  -H 'Content-Type: application/json' \
  -d '{"topics": ["policy_data"]}'
```

Publish a change. The answer carries the number it was given:

```bash
curl -X POST http://localhost:9010/propagation/changes \
  -H 'Content-Type: application/json' \
  -d '{"changes": [{"destination": "/users",
                    "topics": ["policy_data/users"],
                    "payload": {"alice": "admin"},
                    "reason": "alice promoted"}]}'
```

Ask where a machine has got to, and whether everyone has caught up:

```bash
curl 'http://localhost:9010/propagation/cursors?subscriber=pdp-1&destination=/users'
curl 'http://localhost:9010/propagation/convergence?destination=/users'
```

Ask for what was missed, by number:

```bash
curl 'http://localhost:9010/propagation/destinations/since?path=/users&after=3'
```

Watch a place as it changes:

```bash
curl -N 'http://localhost:9010/propagation/watch?destination=/users'
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | Nothing about this service is set from the outside. The three limits below are in the code, because changing one changes what the service promises. |

| Limit | Value | What happens at it |
|---|---|---|
| Largest change | 64 KB | A larger one is refused when published |
| Changes kept per place | 1,000, or 256 KB of them | Older ones drop out; a machine that far behind takes the current value in one step instead |
| Wait before catching a machine up | 3 seconds | A machine left behind by a change is caught up after this |

---

## Where it differs from permitio/opal

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **Which of two changes to the same place survives.** In `permitio/opal` each change is
  handled independently and the one that finishes last is the one left in place, so a change
  published first can overwrite one published after it. This port gives every change a
  number when it arrives and applies them in that order, because two changes to one place
  where nobody has decided which is later is a question with no answer, and the place itself
  is the only participant that sees both.
- **Whether the changes one publisher sent together are ordered against each other.**
  `permitio/opal` applies them strictly in the order they were listed, across different
  places. This port orders changes to the same place and leaves changes to different places
  independent, because ordering across places is exactly the wait that lets a later publish
  overtake an earlier one at a place they share, and only one of the two properties can be
  had.
- **What happens to a change that arrives out of order.** `permitio/opal` writes it. This
  port refuses it, records the span it is missing, and asks for that span by number,
  because a machine that cannot tell a new value from an old one arriving late has no way
  to be sure it is up to date.
- **What happens to a change that arrives twice.** `permitio/opal` writes it again. This
  port recognises it and does not, because a value written twice and a value changed twice
  should not look the same to anything watching.
- **Who names a change.** In `permitio/opal` the publisher may supply a name and, if it does
  not, each listening machine invents its own — so two machines report on the same change
  under two different names. Here the place names it, so a report can be matched back to
  what was published.
- **What a publisher is told.** `permitio/opal` answers that the change was accepted. This
  port answers with the number and name it was given, so the publisher can ask afterwards
  whether it arrived.
- **What happens to a change nobody is listening for.** `permitio/opal` accepts it, warns,
  and delivers it to nobody. This port refuses it when it is published, because a change
  addressed to nothing is more likely a mistake in the address than a deliberate no-op.
- **What a change may carry.** `permitio/opal` requires the value to be a document or a
  list. This port accepts any value, including a plain number or a piece of text, because a
  change that sets a count to seven is a real change and wrapping it in a document to get it
  through alters the shape of what is stored.
- **What happens while the machines cannot reach each other.** `permitio/opal` runs several
  servers that reach each other over a shared channel, keeps changes it could not send in a
  queue of fixed size, drops the oldest when that queue is full, and — with a setting turned
  on — drops changes outright during an outage and makes everyone re-read everything
  afterwards. This port has no such channel: a change is written down where it belongs before
  anyone is told, so there is nothing to buffer and nothing to drop.
- **How a listening machine finds out.** `permitio/opal` holds a long-lived two-way socket
  open to each machine. This port delivers through its own numbered path and offers a
  one-way stream for watching. What a watcher can see differs: a watcher joining late sees
  nothing that came before it joined, and the runtime does not promise every message on that
  stream arrives — which is why nothing is delivered through it. A watcher that needs to
  know it saw everything reads the numbers.
- **Whether the two kinds of change are handled the same way.** `permitio/opal` handles rule
  changes through a queue drained one at a time and fact changes through independent tasks,
  so the two have different guarantees. This port rebuilt only the fact half and gives it
  one rule, because a system where the guarantee depends on which kind of change you sent is
  a system whose guarantee is hard to state.
- **Who may publish.** `permitio/opal` requires a signed token on every request and can limit
  which addresses a given publisher may use. This port has no authentication at all, and its
  address is open to anything that can reach it. It was left out because it does not change
  what order anything arrives in, which is what this port is about.
- **Two places whose names differ only by a leading slash.** In `permitio/opal` `users` and
  `/users` are written to the same place but locked under different names, so two changes to
  them are not held apart. This port treats a name as the name it was given and nothing
  else, so those are two different places.
- **Two places where one name begins with the other.** In `permitio/opal` `/users` and
  `/users_meta` are held apart from each other even though neither contains the other. Here
  they are independent, because they are different places.
- **The multi-tenant arrangement, where every address carries a tenant's name.**
  `not checked`. It exists in `permitio/opal` and this port does not attempt it.
- **Whether the shared channel between `permitio/opal` servers reorders changes in
  transit.** `not checked` — it needs that channel running, which was out of reach here.
  This port has no equivalent to compare against.

---

## Licence

`permitio/opal` is Apache-2.0, © 2021 Or Weis and Asaf Cohen. This port reimplements the
behaviour without copied source; see `ACKNOWLEDGEMENTS.md`.
