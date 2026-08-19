# opal-akka

Sends a change once and has every machine that cares about it end up holding the same
thing, in the same order, including the ones that were switched off at the time.

A port of [permitio/opal](https://github.com/permitio/opal) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

permitio/opal keeps a fleet of machines up to date with the rules and the data they use to
decide who is allowed to do what. One machine publishes a change; every machine watching
that part of the data is told, fetches it, and stores it.

It was ported to derive a way of writing down what a system does that is precise enough for
somebody else to rebuild it from the writing alone — the port is the vehicle, the writing is
the deliverable.

The specifications this port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness) under
`opal-port/`.

---

## permitio/opal → this port

📉 2,276 lines of Python → **840** lines of Java<br>
📁 15 files → **16** files<br>
⚡ 15,457,250 ns to accept one change → **13,371,000** ns<br>
⚡ 31,124,150 ns from a change to a machine holding it → **31,205,900** ns<br>
🎯 13 destinations compared against the original → **13 agree**<br>
🧪 15 of the original's own checks over this part run on this machine → **65**<br>
🔁 0 changes recovered by a machine that was switched off → **50**<br>
🔢 0 changes that carry their place in the order → **all of them**

Full method, the spread behind those medians, and the numbers that did *not* make this
list: [`bench/REPORT.md`](bench/REPORT.md).

Two of those lines go the wrong way and stay in. This port is not faster: the two systems
are within 16% of each other on the operations both have, and this one's slowest changes
take half as long again as the original's. It also has one more file than the part of the
original it replaces.

---

## What it took to build

⏱️ **1.9 hours** from the first command to the published repository, **1.6** of them active<br>
💬 **384** exchanges with the model<br>
✍️ **396,261** tokens written by the model, **105,586,067** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **65** tests

```bash
python toolkit/tokens.py --port opal    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

- **A change gets a number, and the numbers never skip or repeat.** Whoever sent it and
  however many were sent at once, the changes in one group come out in one order that
  everybody agrees on.
- **A machine applies changes in that order and never out of it.** One that arrives twice
  is counted and thrown away. One that arrives too early is written down as a hole and left
  unapplied, rather than being put in ahead of what belongs before it.
- **A machine that comes back gets exactly what it missed.** Not a fresh copy of everything
  and not a shrug — the changes it did not see, in the order they happened.
- **A change addressed to a name reaches everyone watching that name or anything above it.**
  `people/staff/keys` reaches a machine watching `people` and a machine watching
  `people/staff`, and does not reach one watching `people/staff/keys/private`.
- **Anyone can ask how far behind each machine is.** How many changes it has applied, how
  many arrived twice, how many holes it has seen, and where it has got to.

---

## Design decisions

**A number on every change.** Two machines cannot agree on what order things happened in
unless the things say what order they happened in. Every machine can then tell whether it
has the whole story or is missing a piece.

**One writer per group.** Handing out the numbers from one place is the only way to be sure
two changes never get the same one. It costs speed on a single group, and buys an order
nobody has to guess at.

**A machine keeps its own place.** Each one remembers the last change it dealt with, so
sending it the same change twice does nothing and sending it one too early is noticed.
Nothing has to be sent exactly once for the result to be right.

**Recent changes are kept, not just the latest.** A machine that was switched off can be
handed the ones it missed rather than being started again from scratch. If it was off for
too long it is told that, instead of being handed a story with pieces missing.

**A place to put something is a path, not a word.** `/user` and `/users` are two different
places, not one place that happens to start the same way. A change to one has nothing to do
with a change to the other.

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

**3. Open** http://127.0.0.1:9011/fleet.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

No key for any model provider: nothing here calls one.

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9011**.

### Try it

```bash
# a machine joins, watching everything under people
curl -X POST http://127.0.0.1:9011/members/machine-a \
  -H 'Content-Type: application/json' \
  -d '{"channel":"people","watching":["people"]}'

# a change is published
curl -X POST http://127.0.0.1:9011/changes \
  -H 'Content-Type: application/json' \
  -d '{"reason":"a new key","entries":[
        {"address":"people/staff/keys","destination":"/keys/ada","value":"ssh-ed25519 AAAA"}]}'

# what that machine now holds, and how far along it is
curl http://127.0.0.1:9011/members/machine-a

# follow the group as changes arrive, picking up where you left off if the line drops
curl -N http://127.0.0.1:9011/channels/people/stream
```

### What the service answers

| | |
|---|---|
| `POST /changes` | publish one change |
| `POST /members/{id}` | join, and catch up on everything so far |
| `GET /members/{id}` | what that machine holds and where it has got to |
| `POST /members/{id}/leave` | take a machine off, keeping what it holds |
| `POST /members/{id}/return` | put it back, and hand it what it missed |
| `GET /fleet` | how far along every machine is |
| `GET /channels/{group}/changes?after=N` | the changes after a given number |
| `GET /channels/{group}/stream` | the same, as a stream that stays open |

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | nothing here is configured by the surroundings; how many changes a group keeps, and how much room they may take, are in the code and in the specification alongside it |

---

## Where it differs from permitio/opal

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **A change published while a machine was switched off.** permitio/opal never delivers it:
  the machine comes back, asks for a fresh copy of the starting data, and is left holding
  the old value while its neighbours hold the new one, with nothing telling it so. This port
  keeps recent changes and hands back exactly the ones that machine missed, in order,
  because a fleet that quietly disagrees with itself is worse than one that is slow.
- **What a change carries.** permitio/opal's change carries an identifier, a list of
  instructions, a reason and a list of places to call back. It carries nothing that says
  where in the order it belongs. This port adds a number, because everything else on this
  list rests on it.
- **A machine falling further behind than what is kept.** permitio/opal keeps nothing, so
  the question does not come up for it. This port answers with the oldest change it still
  has, and does not return a shortened list, because handing back part of a story reads
  exactly like handing back all of it.
- **A change addressed across two unrelated groups.** permitio/opal accepts one, and gives
  it no number at all. This port refuses it and asks for two, because a change belonging to
  two orders would need two numbers, and a machine watching one of the groups could apply
  half of it.
- **Two changes to the same place at the same time.** In permitio/opal the one that lands
  last is the one that reached a lock first. Twelve changes sent in order landed in order in
  five runs out of five when measured, so in practice the order holds — but nothing in
  permitio/opal says it will. Here the higher number wins, whenever they arrive and in
  whatever order.
- **A change to the very top of a machine's store.** In permitio/opal, fetching the whole
  rule set writes over everything at the top, which removes every data change that machine
  had applied, and says nothing. Measured twice, with the outcome depending on which of the
  two arrived last. Here a change to the top is one more change with a number, applied in
  its turn like any other.
- **Two places whose names start the same.** permitio/opal compares them as words, so a
  change to `/user` holds up a change to `/users` until it finishes. Here they are compared
  as paths and have nothing to do with each other. **Both systems end up holding the same
  values at both places** — the difference is only in what waits for what.
- **How the store is shaped.** permitio/opal stores a document at each place and nests it.
  This port stores the value it was given. Not checked beyond the values themselves: the
  comparison run against the original compared which value ended up where, not how a store
  nests what it is handed.
- **Watching two groups at once.** permitio/opal lets one machine watch names from anywhere.
  Here a machine watches names in one group, and a machine wanting two is two machines,
  because a place in the order is per group and one machine holding two of them is properly
  in step with neither.
- **Fetching a value from somewhere else.** permitio/opal will fetch a value from a web
  address if the change does not carry one, and ships eight ways of doing it. This port
  takes the value in the change. Not a difference in how propagation behaves — it is
  something this port never attempted, and it is listed here because a reader comparing the
  two would otherwise expect it.
- **Everything the original does around this.** Watching a source-control repository,
  running a rules engine, tenants, signing in. None of it is here, and none of it is
  claimed. `docs/scope.md` in the harness repository says what was left out and why.
- **How a machine is reached.** permitio/opal holds a long-lived two-way connection to every
  machine. This port hands changes to each machine directly and offers a stream to anything
  watching from outside, which carries each change's number so a reader that loses the line
  says where it got to and picks up from there. **Not checked:** how the two behave under a
  slow reader that cannot keep up.

---

## Licence

permitio/opal is Apache-2.0, © 2021 Or Weis and Asaf Cohen. This port reimplements the
behaviour without copied source and is Apache-2.0 itself; see
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
