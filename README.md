# opal-akka

Keep a fleet of permission engines up to date: watch a repository for rule changes, take fact
changes over a web address, and push both out to every engine listening — live, in order, and
without any engine having to ask.

A complete port of [permitio/opal](https://github.com/permitio/opal) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

`permitio/opal` is two programs. A **server** watches a git repository or a bundle server for
rule changes, builds the bundles a permission engine reads, and announces every change over a
long-lived connection. A **client** listens on that connection, fetches what changed, writes it
into the permission engine it looks after, fetches facts from wherever the configuration points
it, and reports what it did.

All of it is here. Not one capability of it: both programs, all 47 of their web addresses, the
connection protocol between them, all 177 configuration entries, both permission engines, all
six command-line commands, and both of the screens the original serves.

The written specification — every rule, what established it, and what checks it — lives in a
separate repository, `akka-specify-harness`, under `opal-port/`. It is private for now.

---

## permitio/opal → this port

📉 16,783 Python lines → **19,101 Java lines**<br>
📁 165 files → **132 files**<br>
🎯 294 answers compared → **294 of 294 agree**<br>
🔍 234 elements of the original's surface → **234 covered**<br>
📋 267 rules written down → **404**<br>
⚙️ 177 configuration entries → **177, and 174 of them change something**<br>
⚡ 5.96 → **2.39** microseconds, working out who a change is addressed to<br>
⚡ 10.02 → **1.33** milliseconds, writing a whole rule bundle into an engine<br>
⚡ 3.78 → **2.17** milliseconds, building a rule bundle from a commit<br>
🧪 0 rules broken on purpose to check a test notices → **25**<br>
✅ 45 tests → **341**

This port is **14% larger** than the part of the original it replaces, and that is the honest
direction: about 1,500 lines of it are behaviour the original buys from libraries — the
connection protocol, the metric wire format, the trace payload, the log formatting and the
permissive url splitter — which do not count on the original's side, and about 2,300 more are
what a second module-by-module walk over the original found the first walk had missed.
[`bench/REPORT.md`](bench/REPORT.md) §3 says where every line went.

Full method, and the numbers that did not make this list: [`bench/REPORT.md`](bench/REPORT.md).

---

## What it took to build

⏱️ **173.3 hours** from the first command to the published repository, **15.0** of them active<br>
💬 **4,850** exchanges with the model<br>
✍️ **4,216,374** tokens written by the model, **2,124,700,000** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **341** tests

```bash
python toolkit/tokens.py --port opal
```

The record of every question, and where the time went, lives with the specification.

---

## What it does

**The server**

- **Watches a policy repository** — git over HTTPS or SSH, or a bundle server serving tar
  archives — on a timer, on a webhook from GitHub, GitLab, Bitbucket, Gitea or Azure, or when
  asked directly.
- **Builds rule bundles**: everything at one commit, or only what changed between two, with the
  deletions ordered so a rule is removed before the rules that depend on it.
- **Announces changes** on a long-lived connection, addressed by topic. A change to
  `users/keys` reaches anybody listening to `users`.
- **Takes fact changes** from any program allowed to send them, and fans them out to exactly
  the clients that asked for that topic.
- **Mints and verifies the tokens** the fleet uses, and publishes the public half of its signing
  key so anybody can check one.
- **Serves many tenants at once**, each with its own repository, its own bundles and its own
  clients, sharing one clone on disk where two tenants name the same repository.
- **Reports on the fleet**: which clients are connected, from where, listening to what.

**The client**

- **Keeps one permission engine current** — OPA or Cedar, run as a child process or reached over
  the network.
- **Applies rule changes** as whole bundles or as differences, and **fact changes** as a whole
  value or as a patch.
- **Fetches facts** from anywhere the configuration points it, through a set of fetchers a
  deployment can extend.
- **Writes a log of every change into the engine itself**, so "is this engine current?" is a
  question the engine can answer.
- **Keeps working with no server**: it saves what it holds and restores it at start-up, which is
  what lets a permission engine come up in a network that cannot reach anything.
- **Tells whoever asked** what it did with a change, at the addresses they registered.

---

## Where it differs from permitio/opal

Every behaviour that may differ, listed as a decision. A behaviour nobody checked says so.

- **The rendered screens are fed by a stream, not by polling.** `RENDERING.md` R1 requires it.
  What a caller can see changes: a poller misses nothing because it re-reads, and a stream
  either replays or drops. Here the two screens draw a document that cannot change while the
  process runs, so nothing is missed either way — but the obligation to say so is the same.
  **Not checked:** what a real OPAL client sees across a dropped connection is unchanged,
  because the client protocol was not moved to a stream; only the rendered screens were.
- **One deployable is both programs.** OPAL ships `opal-server` and `opal-client` as separate
  distributions. Here `OPAL_ROLE` selects `server`, `client` or `both`; each role mounts exactly
  the routes the original's own route table has, and under `both` the client's are mounted under
  `/client`. Run separately, the surfaces are identical.
- **Scopes are entities, not Redis keys.** `REDIS_URL` is kept, reported by `print-config`, and
  read by nothing. The capability — a tenant's configuration survives a restart and every
  replica sees the same one — is the runtime's.
- **Replicas see each other's publications without a backbone.** OPAL, with `BROADCAST_URI`
  unset and more than one replica, has a split fleet. Here they still see each other.
  `BROADCAST_URI` selects an outbound relay for interoperating with a real OPAL server sharing
  one.
- **`GET /scopes/{id}/policy` answers 409 where the original answers 503**, when the clone
  exists and the branch the tenant named is not in it. The original's answer says retry, and no
  amount of retrying resolves a branch that is not there.
- **A base hash the repository does not hold is served the full bundle.** The original answers
  500, by two routes. A client asks for that after its store is wiped, and the full bundle is
  the only answer that lets it recover.
- **A webhook payload carrying all nine URL fields is handled.** The original's reader calls
  `list.remove(None)` unconditionally and answers 500 for such a payload. This filters instead.
- **Offline backup is written.** The original's `backup_store()` raises on every call under its
  own pinned dependency floor, so the file is never written; this writes it, atomically, as the
  capability's own configuration entry describes.
- **`GET /internal/metrics` is a route the original does not have.** Every measurement OPAL
  sends to a monitoring agent is sent — the same datagrams, the same trace payload — and also
  recorded here, because the wire is write-only and a fleet operator diagnosing one replica
  otherwise has nothing to ask.
- **A line of engine output that is not JSON is logged as text.** The original logs the
  undecoded bytes, so its line reads `b'...'`.
- **A refused archive member names the link kind.** Both refuse a link pointing outside the
  extraction directory; this says `symlink` or `link` the way the original does, which it did
  not until the benchmark compared them.
- **The policy watcher runs on one process per machine**, chosen by a lock on a file, exactly as
  the original chooses among its forked workers. On a cluster spanning machines, that is one
  watcher per machine rather than one per cluster. **Not checked:** what two machines both
  holding a watcher do to one repository, because that needs two machines.
- **Seven configuration entries change nothing, and neither do they in the original.**
  `LOG_SHOW_CODE_LINE`, `NO_RPC_LOGS`, `OPA_HEALTH_CHECK_TRANSACTION_LOG_PATH`, `OPAL_WS_TOKEN`
  and `OPAL_WS_LOCAL_URL` are declared and unread on both sides; `HTTP_FETCHER_PROVIDER_CLIENT`
  chooses between two HTTP libraries the original has and this does not; `LOG_DIAGNOSE`
  additionally turns on git's own protocol tracing, which this rebuild's git library does not
  shell out to.
- **Six entries describing how a command-line invocation would bind a server change nothing
  here.** `SERVER_HOST`, `SERVER_PORT`, `SERVER_BIND_PORT`, `SERVER_WORKER_COUNT`,
  `CLIENT_API_SERVER_HOST`, `CLIENT_API_SERVER_PORT` and `CLIENT_API_SERVER_WORKER_COUNT` are
  read by the original's own process runner; here the runtime binds the port. All are kept and
  reported by `print-config`, and `SERVER_PORT` is still carried into `SERVER_BIND_PORT` the way
  the original carries it.
- **No Datadog agent has accepted either payload.** Both are reproduced from what the original's
  own client put on a socket, and no agent was available to accept one. **Not checked.**
- **The three broadcast transports are constructed and not connected.** Redis, Kafka and
  Postgres are selected by scheme and their reader loops are covered in process; what happens
  against real ones is **not checked**.
- **A client asked to reconcile is closed with an ordinary close, not with 1012.** The source
  closes each connection with the "service restart" status so a client can tell a reconciliation
  from a network drop. The websocket handler on this target is a flow of text frames and has no
  close frame to give a code to, so the close carries the default. A client reconnects and
  refetches either way, which is what the code is for; what it cannot do is tell the two apart in
  its own log.
- **The clone wait is never abandoned because the caller hung up.** The source polls
  `request.is_disconnected()` while it holds a request waiting for a scope's clone, and gives the
  slot back when nobody is listening. This target hands a handler a finished request and takes an
  answer back; it does not report that the connection went away underneath. The wait is bounded
  and capped the same way, and its `disconnected` outcome is therefore never counted.
- **The rendered pages take their prefix from a header, not from the web server.** Behind a proxy
  that mounts the process under a path, the source builds the document's address from the root
  path its web server was started with. There is no such argument here, so the prefix is read
  from `X-Forwarded-Prefix` — the header the proxy sends to say what it stripped. With no proxy
  in front the page is byte for byte what the source serves.
- **The set of scopes lives in an entity of its own, and that bounds how many there can be.**
  OPAL lists its scopes by scanning Redis's keyspace, which has no such bound. Here the ids are
  one entity's state, which this runtime replicates up to a megabyte — about twenty-five thousand
  ids of the length a tenant name has. A deployment past that would need the listing split across
  several. **Not checked:** what this runtime does at that ceiling, because reaching it needs
  twenty-five thousand tenants.
- **A traceback carries no local variables.** `LOG_DIAGNOSE` turns on loguru's rendering of the
  locals at each frame. A Java stack trace has no such thing, so the entry turns on the git
  transport's own tracing — which is the other half of what the source uses it for — and the
  rendered locals have no counterpart.
- **A web-server line that the source prints twice is printed once.** With
  `LOG_PATCH_UVICORN_LOGS` off, the source leaves its web server's loggers with their own
  handlers *and* propagating to the root, so a line appears twice in two shapes. This target's
  web server installs no handlers of its own, so the same line appears once, in this format.
- **An Ed25519 or EC private key is enough to sign with.** The source's key caster loads all
  three families and its JWT library signs from the private key alone. This derives the public
  half rather than asking for it, which is the same input and the same output; what differs is
  that the rebuild needs one more library on its classpath to do it, named in `pom.xml`.
- **`GET /scopes` is one read per scope.** So is the original's: its listing scans the keyspace
  and reads each key. Named here because a reader who expects one query will otherwise find it
  and think it a defect.
- **What a fleet of a hundred clients does.** Every measured comparison is in-process. The
  behaviour under real load is **not checked**.

---

## Design decisions

**Two programs, one deployable.** The two halves share a configuration system, a schema set, a
git layer and a topic model — about half the code. Splitting them into two deployables would
have meant either two copies of that or a library between them; one deployable with a role
switch keeps one copy, and running it twice with different roles gives the original's own two
processes.

**The connection protocol is implemented, not wrapped.** OPAL's clients talk a specific
websocket RPC protocol, three libraries deep. A port that invented its own protocol would be a
different product; this speaks that one, so a real `opal-client` can connect to this server and
this client can connect to a real `opal-server`.

**A tenant's clone is named after its repository, not after the tenant.** A hundred tenants on
one policy repository cost one clone on disk and one network fetch per pass, and a purge checks
whether any surviving tenant still names a repository before removing it.

**Everything a repository can do to you is bounded.** One git operation at a time per
repository, a ceiling on how many run at once, a ceiling on how many may be abandoned, a
timeout on the wait, and a delay that doubles before a repository that keeps failing is tried
again. Without those, a handful of dead repositories account for thousands of clone attempts an
hour.

**A publication made while the backbone is down goes nowhere.** Delivering it locally would
leave one replica's clients holding a change no other replica has, for as long as the outage
lasts. Dropping it leaves every client on the old answer, and the reconciliation that runs when
the backbone comes back moves all of them together.

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

**3. Open** http://localhost:9101/docs.

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

341 tests: 267 that need no runtime, and 74 that start one.

### Run it

```bash
OPAL_ROLE=both \
OPAL_POLICY_REPO_URL=https://github.com/permitio/opal-example-policy-repo \
mvn compile exec:java
```

Then:

- `http://localhost:9101/docs` — the API browser, which is the screen the original serves
- `http://localhost:9101/healthcheck` — whether this process is fit to be given work
- `http://localhost:9101/policy?path=.` — a rule bundle for everything

### The command line

```bash
mvn -q compile exec:java -Dexec.mainClass=io.akka.opal.cli.OpalCli -Dexec.args="print-config"
mvn -q compile exec:java -Dexec.mainClass=io.akka.opal.cli.OpalCli -Dexec.args="generate-secret"
mvn -q compile exec:java -Dexec.mainClass=io.akka.opal.cli.OpalCli -Dexec.args="obtain-token TOKEN"
```

---

## Where things are

```
src/main/java/io/akka/opal/
  common/      what both halves share: configuration, schemas, git, topics,
               authentication, fetchers, logging, measurements, tracing
  server/      the watcher, the bundle server, the connection broker, the
               tenants, the statistics, the webhook
  client/      the updaters, the policy store, the engines, the callbacks
  api/         the web addresses, and the shapes their answers take
  cli/         the six commands
```

---

## Licence

`permitio/opal` is Apache 2.0. This rebuild carries the same licence, and
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md) names every string, shape and wording taken from
it — checked by running `python toolkit/copied_strings.py opal`, not from memory.
