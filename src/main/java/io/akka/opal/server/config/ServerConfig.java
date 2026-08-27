package io.akka.opal.server.config;

import io.akka.opal.common.auth.Types.EncryptionKeyFormat;
import io.akka.opal.common.confi.Confi;
import io.akka.opal.common.confi.ConfiEntry;
import io.akka.opal.common.config.Enums.PolicyBundleServerType;
import io.akka.opal.common.config.Enums.PolicySourceTypes;
import io.akka.opal.common.config.Options.GitWebhookRequestParams;
import io.akka.opal.common.schemas.Data.DataSourceConfig;
import io.akka.opal.common.schemas.Data.DataSourceEntry;
import io.akka.opal.common.schemas.Data.ServerDataSourceConfig;
import java.util.List;
import java.util.Map;

/**
 * The 82 entries only the server has. SPEC-002 R6.
 *
 * <p>Generated from the census taken off the running source by
 * {@code opal-port/probes/complete/gen_config.py}, so that the set is the source's own
 * rather than a transcription of it. Declaration order is the source's declaration
 * order, which is also evaluation order.
 */
public final class ServerConfig extends Confi {

  @Override
  protected String configName() {
    return "OpalServerConfig";
  }


  public ServerConfig(Map<String, String> environment) {
    super("OPAL_", environment);
    declare();
    resolveDelayed();
  }

  public ServerConfig() {
    this(io.akka.opal.common.confi.ConfigFiles.overlay(System.getenv()));
  }

  private void declare() {
    str("OPAL_WS_LOCAL_URL", "WS_LOCAL_URL", "ws://localhost:7002/ws", "The local WebSocket URL for OPAL", null);
    str("OPAL_WS_TOKEN", "WS_TOKEN", "THIS_IS_A_DEV_SECRET", "The WebSocket token for OPAL", null);
    str("CLIENT_LOAD_LIMIT_NOTATION", null, "If supplied, rate limit would be enforced on server's websocket endpoint. Format is `limits`-style notation (e.g '10 per second'), see link: https://limits.readthedocs.io/en/stable/quickstart.html#rate-limit-string-notation");
    str("BROADCAST_URI", null, "The URL for the backbone pub/sub server");
    str("BROADCAST_CHANNEL_NAME", "EventNotifier", "The name to be used for segmentation in the backbone pub/sub");
    bool("BROADCAST_RECONNECT_ENABLED", true, "Reconnect the broadcaster reader on a backbone disconnect instead of dropping all client connections. Set to False to revert to the legacy (non-reconnecting) broadcaster.");
    integer("BROADCAST_RECONNECT_MAX_RETRIES", 0, "Maximum consecutive broadcaster reconnect attempts before giving up and letting the worker restart (0 = retry forever).");
    decimal("BROADCAST_RECONNECT_BACKOFF_MIN_SECONDS", 0.5, "Minimum backoff in seconds between broadcaster reconnect attempts.");
    decimal("BROADCAST_RECONNECT_BACKOFF_MAX_SECONDS", 30.0, "Maximum backoff in seconds between broadcaster reconnect attempts.");
    integer("BROADCAST_REPLAY_BUFFER_SIZE", 10000, "Max number of outbound broadcasts buffered while the backbone is down and replayed on reconnect (0 disables buffering). On overflow the oldest buffered broadcasts are dropped; the resync on reconnect still reconciles clients.");
    bool("BROADCAST_RESYNC_ON_RECONNECT", true, "After a backbone gap that may have lost updates, force this worker's connected clients to reconnect so they re-fetch full policy + data state (guarantees cross-instance consistency).");
    decimal("BROADCAST_RESYNC_SETTLE_SECONDS", 2.0, "Grace period after a broadcaster reconnect before replaying buffered broadcasts and resyncing clients, to let peer servers re-subscribe.");
    bool("BROADCAST_HEALTHCHECK_ENABLED", true, "Make /healthcheck reflect the broadcaster reader's health so a k8s readiness/liveness probe can route away from or restart a worker whose reader is wedged while clients depend on it. Set to False to revert /healthcheck to always returning ok.");
    bool("BROADCAST_FREEZE_ON_DISCONNECT", true, "During a broadcaster backbone gap, freeze client-facing publishes on every worker instead of applying them locally \u2014 so a write that cannot fan out to the whole fleet is never served by just one worker (fleet consistency over freshness). Recovery is the reconnect resync: clients re-fetch their configured data sources and policy, so updates covered by those are reconciled; one-off updates outside them (inline data payloads, ad-hoc fetch URLs) are DROPPED by a freeze, not deferred. Internal coordination topics (statistics, keepalive, the git webhook trigger) are exempt and keep the deliver-locally + buffer-for-replay behavior. Engages only with BROADCAST_RECONNECT_ENABLED (without the reconnecting broadcaster there is no gap signal, so the flag is a no-op) and requires BROADCAST_RESYNC_ON_RECONNECT (if the resync is disabled, freezing is refused with a warning). Set to False for the previous behavior, where the receiving worker's own clients update immediately and peers only after reconnect.");
    enumeration("AUTH_PRIVATE_KEY_FORMAT", EncryptionKeyFormat.class, EncryptionKeyFormat.pem, "The format of the private key for authentication");
    str("AUTH_PRIVATE_KEY_PASSPHRASE", null, "The passphrase for the private key");
    key("AUTH_PRIVATE_KEY", "The private key for authentication",
            () -> get("AUTH_PRIVATE_KEY_FORMAT"), false);
    str("AUTH_JWKS_URL", "/.well-known/jwks.json", "The URL for the JSON Web Key Set (JWKS)");
    str("AUTH_JWKS_STATIC_DIR",
            System.getProperty("user.dir") + "/jwks_dir", "The directory for static JWKS files");
    str("AUTH_MASTER_TOKEN", null, "The master token for authentication");
    enumeration("POLICY_SOURCE_TYPE", PolicySourceTypes.class, PolicySourceTypes.Git, "Set your policy source can be GIT / API");
    str("POLICY_REPO_URL", null, "Set your remote repo URL e.g:https://github.com/permitio/opal-example-policy-repo.git        , relevant only on GIT source type");
    str("POLICY_BUNDLE_URL", null, "Set your API bundle URL, relevant only on API source type");
    str("POLICY_REPO_CLONE_PATH",
            System.getProperty("user.dir") + "/regoclone", "Base path to create local git folder inside it that manage policy change");
    str("POLICY_REPO_CLONE_FOLDER_PREFIX", "opal_repo_clone", "Prefix for the local git folder");
    bool("POLICY_REPO_REUSE_CLONE_PATH", false, "Set if OPAL server should use a fixed clone path (and reuse if it already exists) instead of randomizing its suffix on each run");
    str("POLICY_REPO_MAIN_BRANCH", "master", "The main branch of the policy repository");
    str("POLICY_REPO_SSH_KEY", null, "The SSH key for the policy repository");
    str("POLICY_REPO_MANIFEST_PATH", "", "Path of the directory holding the '.manifest' file (new fashion), or of the manifest file itself (old fashion). Repo's root is used by default");
    integer("POLICY_REPO_CLONE_TIMEOUT", 0, "The timeout for cloning the policy repository (0 means wait forever)");
    decimal("SCOPES_GIT_FETCH_TIMEOUT", 120.0, "Soft timeout in seconds for a single scope git clone/fetch: the awaiting operation is abandoned (the event loop and the sync pass move on) and the op is logged and skipped, retried next cycle. It is a SOFT timeout: the underlying git call keeps running on its own thread, and the pinned libgit2 sets no socket/server read timeout, so a black-holed remote can keep that thread \u2014 and the source's in-flight marker \u2014 alive for the life of the process (that source is then skipped until it recovers or the process restarts); SCOPES_GIT_MAX_ZOMBIES bounds how many such threads accumulate. Either way one unreachable repo never blocks boot or other scopes (0 = no timeout).");
    integer("SCOPES_GIT_MAX_WORKERS", 10, "Maximum number of concurrent scope git operations. It bounds phase 1 (the network clone/fetch of each distinct repo) AND phase 2 (the local change-check of scopes that reuse an already-cloned repo), so it sets how many scopes are synced at once in either phase. A timed-out operation stops counting against this limit, so one hung remote does not hold a concurrency slot \u2014 its lingering daemon thread persists on its own (a black-holed remote's can persist for the life of the process). That tail is bounded by SCOPES_GIT_MAX_ZOMBIES, which is a GLOBAL ceiling: read its description, because at that ceiling new git ops are refused for every scope, healthy ones included.");
    decimal("SCOPES_GIT_PRELOAD_DRAIN_TIMEOUT", 10.0, "Max seconds the pre-fork scope preload waits for in-flight git ops to finish before tearing down and forking workers. Ops still lingering past this bound are left running on their daemon threads and their cached handles are left unfreed by the pre-fork cache reset, avoiding a use-after-free (0 = don't wait).");
    integer("SCOPES_GIT_MAX_ZOMBIES", 40, "Maximum number of in-flight scope git operations (live plus lingering timed-out) allowed to hold a daemon thread at once, counted GLOBALLY across all sources. It is a last-resort ceiling on thread growth when remotes hang, not a per-source guard \u2014 that is handled separately, by skipping a source that already has an operation in flight. Once the ceiling is reached, new git ops are refused (logged, and retried next cycle) for EVERY scope, healthy ones included, until enough threads drain; with remotes that never return, that state can persist. Set it well above SCOPES_GIT_MAX_WORKERS and alert on the refusal log (0 = no cap; a negative value is clamped to 0 and also means no cap, at the cost of unbounded thread growth during an outage).");
    decimal("SCOPES_GIT_BACKOFF_BASE_SECONDS", 10.0, "First delay before the periodic sync pass re-attempts a SOURCE whose git clone or fetch just failed (unreachable host, revoked credentials, deleted repo); every further consecutive failure doubles it \u2014 10s, 20s, 40s, ... minutes, hours, days \u2014 with no upper bound unless SCOPES_GIT_BACKOFF_MAX_SECONDS is set. A delay shorter than the gap to the next pass simply does not skip that pass, so the first few doublings cost one attempt per pass exactly as before and the schedule bites from roughly the fourth consecutive failure. It exists because nothing else records a failure: without it every pass re-attempts every dead repo, and so does every duplicate scope sharing that repo \u2014 a handful of dead repositories can account for thousands of clone attempts per hour. Only the periodic pass and the boot preload honour it (in both phases, and re-checked under the source lock, so duplicates of a source that fails in a pass cost one attempt, not one per scope): an explicit POST /scopes/:scope_id/refresh, POST /scopes/refresh or PUT /scopes attempts the source immediately, so an operator who has just repaired credentials recovers at once, and any successful clone or fetch clears both the delay and the consecutive-failure count. The state is in-memory and per process \u2014 it resets on restart. When a periodic pass runs (POLICY_REFRESH_INTERVAL > 0) a forked worker inherits whatever the pre-fork preload recorded, so the leader does not re-hammer repos that already failed at boot; without one, the boot sync is the only pass-originated sync, so it drops the inherited entries and attempts every source once. Watch opal_server.scopes.sources_in_backoff (gauge of sources currently being skipped, tagged by pid) and opal_server.scopes.git_op_skipped with reason:backoff (counter); the WARNING logged when a source enters backoff, and again when its delay first exceeds a day, names the repository. 0 or negative disables the backoff entirely \u2014 nothing is recorded and nothing is skipped; nan and inf are treated as disabled too, because they parse cleanly rather than failing the process at startup and neither is a duration.");
    decimal("SCOPES_GIT_BACKOFF_MAX_SECONDS", 0.0, "Optional cap on the per-source doubling backoff of SCOPES_GIT_BACKOFF_BASE_SECONDS. 0 (the default), negative, nan or inf means NO cap: a repository that has been unreachable for a day is checked again in two, then four, and before long only at the next restart or explicit refresh \u2014 a repository that keeps failing is, in all likelihood, dead. Set a positive value to bound instead how stale a repository that comes back on its own (without anyone touching its scope) can get: the periodic pass then re-attempts it at most that long after the previous attempt. A value below the base is floored at the base (one pass at a time), never inert. Lowering the cap at runtime is not retroactive for delays already armed.");
    decimal("SCOPES_POLICY_CLONE_WAIT_SECONDS", 20.0, "How long GET /scopes/:scope_id/policy holds a request while that scope's clone is still being populated, before falling through to the 503 + Retry-After it answers today. The route re-checks once a second and returns the bundle the moment the clone is usable. It exists because the opal-client PDP ignores Retry-After: it makes five attempts with random-exponential backoff capped at 10s (~20-40s of coverage) and then stays quiet until the next pub/sub policy message or a reconnect, so a clone that outlives those attempts strands that PDP with no policy \u2014 and the update-all published when a clone completes names only the scope that was syncing, so siblings sharing the same clone are not woken. Holding the request converts that gap into latency the client already tolerates: five client attempts against a 20s hold cover about two minutes of clone time, so short and medium re-clones \u2014 meaning the download phase; the rmtree-and-init window before it answers 503 + Retry-After 5 and is not waited on \u2014 produce no client-visible gap. What this bounds is the WAIT plus at most one more bundle attempt: time queued behind other bundle builds on the shared executor is outside the deadline, which is what SCOPES_POLICY_CLONE_WAIT_MAX_INFLIGHT bounds instead. The budget matters in both directions \u2014 20s is well under the 60s ALB idle timeout (a longer hold surfaces as a 504, which the client cannot tell apart from a dead server) and far under the client's 300s aiohttp total timeout. Readiness is derived from DISK (the clone still has no remote-tracking refs), never from an in-process marker, so every worker answers alike: the clone runs in the leader while this route is served by any worker. The hold is an awaited sleep loop, so it occupies no thread between polls and leaves the event loop and the gunicorn worker heartbeat unaffected; it is abandoned early if the client disconnects. 0 or negative disables the wait (answer 503 immediately). nan, inf and -inf are treated as disabled too: unlike a non-numeric value, which fails this process at startup when the environment is parsed, they parse cleanly \u2014 inf would otherwise mean the clamped maximum hold on every clone-in-progress request, and nan is not a budget at all. Values above 55s are clamped to 55s so the hold can never outlive the load balancer's idle timeout.");
    integer("SCOPES_POLICY_CLONE_WAIT_MAX_INFLIGHT", 64, "Maximum number of requests one worker process may hold at once inside the SCOPES_POLICY_CLONE_WAIT_SECONDS wait. Requests beyond the cap get the immediate 503 + Retry-After 30 they would have got before the wait existed, so the cap can never make things worse than not waiting. It exists because polling is cheap but RELEASING is not: when the clone lands, every held request builds a full bundle on the loop's DEFAULT executor: about min(32, cpu+4) threads shared by every off-loop call this process makes, in front of an unbounded queue. Measured throughput there falls from ~52 bundles/s at 32 concurrent builds to ~18/s at 1000. Scope git clone/fetch does NOT share that pool \u2014 each op runs on its own single-use daemon-thread executor, bounded by SCOPES_GIT_MAX_WORKERS through a semaphore \u2014 so this key is the only bound on how many bundle builds can pile up at once. Size it so the cap divided by the bundles-per-second that pod can really build fits inside the 60s ALB idle timeout MINUS the hold: above that, released requests queue past the timeout and 504 \u2014 the very failure the hold exists to prevent \u2014 and a rolling restart drains worse, because uvicorn waits for in-flight requests while gunicorn SIGKILLs the worker at 30s, dropping every websocket that worker still holds. The count is per process, not per pod: a pod running N workers holds up to N times this number. 0 or negative means no cap.");
    str("LEADER_LOCK_FILE_PATH", "/tmp/opal_server_leader.lock", "The path to the leader lock file");
    enumeration("POLICY_BUNDLE_SERVER_TYPE", PolicyBundleServerType.class, PolicyBundleServerType.HTTP, "The type of bundle server e.g. basic HTTP , AWS S3. (affects how we authenticate with it)");
    str("POLICY_BUNDLE_SERVER_TOKEN", null, "Secret token to be sent to API bundle server");
    str("POLICY_BUNDLE_SERVER_TOKEN_ID", null, "The id of the secret token to be sent to API bundle server");
    str("POLICY_BUNDLE_SERVER_AWS_REGION", "us-east-1", "The AWS region of the S3 bucket");
    str("POLICY_BUNDLE_TMP_PATH", "/tmp/bundle.tar.gz", "Path for temp policy file, need to be writeable");
    str("POLICY_BUNDLE_GIT_ADD_PATTERN", "*", "File pattern to add files to git default to all the files (*)");
    bool("REPO_WATCHER_ENABLED", true, "Enable the repository watcher");
    bool("PUBLISHER_ENABLED", true, "Enable the publisher");
    integer("BROADCAST_KEEPALIVE_INTERVAL", 3600, "the time to wait between sending two consecutive broadcaster keepalive messages");
    str("BROADCAST_KEEPALIVE_TOPIC", "__broadcast_session_keepalive__", "the topic on which we should send broadcaster keepalive messages");
    integer("MAX_CHANNELS_PER_CLIENT", 15, "max number of records per client, after this number it will not be added to statistics, relevant only if STATISTICS_ENABLED");
    str("STATISTICS_WAKEUP_CHANNEL", "__opal_stats_wakeup", "The topic a waking-up OPAL server uses to notify others he needs their statistics data");
    str("STATISTICS_STATE_SYNC_CHANNEL", "__opal_stats_state_sync", "The topic other servers with statistics provide their state to a waking-up server");
    str("STATISTICS_SERVER_KEEPALIVE_CHANNEL", "__opal_stats_server_keepalive", "The topic workers use to signal they exist and are alive");
    str("STATISTICS_SERVER_KEEPALIVE_TIMEOUT", "20", "Timeout for forgetting a server from which a keep-alive haven't been seen (keep-alive frequency would be half of this value)");
    str("SCOPES_PURGE_CHANNEL", "__opal_scope_purge__", "Pub/sub channel (worker-to-worker, over the broadcaster) used to purge GitPolicyFetcher caches fleet-wide when a scope is deleted or repointed to a new source. Every worker subscribes and drops its own cache entries; the leader is the only actor that may authorize that, because only it can check whether a surviving scope still shares the source. It does not remove the clone dir.");
    str("ALL_DATA_TOPIC", "policy_data", "Top level topic for data");
    str("ALL_DATA_ROUTE", "/policy-data", "The route for all policy data");
    strDelayed("ALL_DATA_URL",
            c -> "http://localhost:7002" + c.getString("ALL_DATA_ROUTE"), "URL for all data config [If you choose to have it all at one place]");
    str("DATA_CONFIG_ROUTE", "/data/config", "URL to fetch the full basic configuration of data");
    str("DATA_CALLBACK_DEFAULT_ROUTE", "/data/callback_report", "Exists as a sane default in case the user did not set OPAL_DEFAULT_UPDATE_CALLBACKS");
    modelDelayed("DATA_CONFIG_SOURCES", ServerDataSourceConfig.class,
            c -> new ServerDataSourceConfig(
                new DataSourceConfig(List.of(DataSourceEntry.of(
                    c.getString("ALL_DATA_URL"),
                    List.of(c.getString("ALL_DATA_TOPIC")), ""))), null), "Configuration of data sources by topics");
    str("DATA_UPDATE_TRIGGER_ROUTE", "DATA_CONFIG_ROUTE", "/data/update", "URL to trigger data update events", null);
    str("POLICY_REPO_WEBHOOK_SECRET", null, "The secret for the policy repository webhook");
    bool("POLICY_REPO_WEBHOOK_ENFORCE_BRANCH", false, "Enforce branch name in incoming webhook");
    model("POLICY_REPO_WEBHOOK_PARAMS", GitWebhookRequestParams.class,
            GitWebhookRequestParams.github(), "Parameters for processing the incoming webhook");
    integer("POLICY_REPO_POLLING_INTERVAL", 0, "The polling interval for the policy repository");
    list("ALLOWED_ORIGINS", List.of("*"), "List of allowed origins for CORS");
    list("FILTER_FILE_EXTENSIONS", List.of(".rego", ".json"), "List of file extensions to filter. Example: ['.rego', '.json']");
    list("BUNDLE_IGNORE", List.of(), "List of patterns to ignore in the bundle");
    bool("NO_RPC_LOGS", true, "Disable RPC logs");
    integer("SERVER_WORKER_COUNT", null, "(if run via CLI) Worker count for the server [Default calculated to CPU-cores]");
    str("SERVER_HOST", "127.0.0.1", "(if run via CLI)  Address for the server to bind");
    str("SERVER_PORT", null, "Deprecated, use SERVER_BIND_PORT instead");
    integer("SERVER_BIND_PORT", 7002, "(if run via CLI)  Port for the server to bind");
    bool("ENABLE_DATADOG_APM", false, "Set if OPAL server should enable tracing with datadog APM");
    bool("DEBUG_INTERNAL_STATS", false, "Expose GET /internal/git-fetcher-cache-stats with in-memory cache sizes and process RSS. For diagnostics/tests only; keep off in production.");
    bool("SCOPES", false, "Enable scopes");
    integer("SCOPES_REPO_CLONES_SHARDS", 1, "The max number of local clones to use for the same repo (reused across scopes)");
    str("REDIS_URL", "redis://localhost", "The URL for the Redis server");
    str("BASE_DIR",
            System.getProperty("user.home") + "/.local/state/opal", "The base directory for OPAL");
    integer("POLICY_REFRESH_INTERVAL", 0, "Policy polling refresh interval");
    decimal("SCOPES_STORE_READ_TIMEOUT", 10.0, "Timeout for a scope-store read taken while holding a source's lock \u2014 the sibling check a delete/repoint purge runs before authorizing the fleet-wide cache purge. The Redis client is built without a socket timeout, so without this an unreachable store would pin that lock for the life of the process and block every later sync, purge and delete for the source. On expiry the sibling check fails open, and what that decides is the fleet-wide MEMORY purge only \u2014 the leader no longer touches the clone tree: a DELETE confirms it defensively (its record is already gone, so withholding would strand the fleet's cache entries, while over-purging self-heals on the surviving sibling's next sync), a REPOINT withholds it (the old source's record is still live, just moved). The clone dir is unaffected either way: on a store fault the delete floor KEEPS this worker's clone rather than risk deleting one a live sibling shares, leaving an orphan tracked by PER-15612 (0 or negative means no timeout).");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> OPAL_WS_LOCAL_URL() {
    return (ConfiEntry<String>) entries().get("OPAL_WS_LOCAL_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> OPAL_WS_TOKEN() {
    return (ConfiEntry<String>) entries().get("OPAL_WS_TOKEN");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> CLIENT_LOAD_LIMIT_NOTATION() {
    return (ConfiEntry<String>) entries().get("CLIENT_LOAD_LIMIT_NOTATION");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> BROADCAST_URI() {
    return (ConfiEntry<String>) entries().get("BROADCAST_URI");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> BROADCAST_CHANNEL_NAME() {
    return (ConfiEntry<String>) entries().get("BROADCAST_CHANNEL_NAME");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> BROADCAST_RECONNECT_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("BROADCAST_RECONNECT_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> BROADCAST_RECONNECT_MAX_RETRIES() {
    return (ConfiEntry<Integer>) entries().get("BROADCAST_RECONNECT_MAX_RETRIES");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> BROADCAST_RECONNECT_BACKOFF_MIN_SECONDS() {
    return (ConfiEntry<Double>) entries().get("BROADCAST_RECONNECT_BACKOFF_MIN_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> BROADCAST_RECONNECT_BACKOFF_MAX_SECONDS() {
    return (ConfiEntry<Double>) entries().get("BROADCAST_RECONNECT_BACKOFF_MAX_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> BROADCAST_REPLAY_BUFFER_SIZE() {
    return (ConfiEntry<Integer>) entries().get("BROADCAST_REPLAY_BUFFER_SIZE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> BROADCAST_RESYNC_ON_RECONNECT() {
    return (ConfiEntry<Boolean>) entries().get("BROADCAST_RESYNC_ON_RECONNECT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> BROADCAST_RESYNC_SETTLE_SECONDS() {
    return (ConfiEntry<Double>) entries().get("BROADCAST_RESYNC_SETTLE_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> BROADCAST_HEALTHCHECK_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("BROADCAST_HEALTHCHECK_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> BROADCAST_FREEZE_ON_DISCONNECT() {
    return (ConfiEntry<Boolean>) entries().get("BROADCAST_FREEZE_ON_DISCONNECT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<EncryptionKeyFormat> AUTH_PRIVATE_KEY_FORMAT() {
    return (ConfiEntry<EncryptionKeyFormat>) entries().get("AUTH_PRIVATE_KEY_FORMAT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_PRIVATE_KEY_PASSPHRASE() {
    return (ConfiEntry<String>) entries().get("AUTH_PRIVATE_KEY_PASSPHRASE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_PRIVATE_KEY() {
    return (ConfiEntry<String>) entries().get("AUTH_PRIVATE_KEY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_JWKS_URL() {
    return (ConfiEntry<String>) entries().get("AUTH_JWKS_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_JWKS_STATIC_DIR() {
    return (ConfiEntry<String>) entries().get("AUTH_JWKS_STATIC_DIR");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_MASTER_TOKEN() {
    return (ConfiEntry<String>) entries().get("AUTH_MASTER_TOKEN");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<PolicySourceTypes> POLICY_SOURCE_TYPE() {
    return (ConfiEntry<PolicySourceTypes>) entries().get("POLICY_SOURCE_TYPE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_URL() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_BUNDLE_URL() {
    return (ConfiEntry<String>) entries().get("POLICY_BUNDLE_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_CLONE_PATH() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_CLONE_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_CLONE_FOLDER_PREFIX() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_CLONE_FOLDER_PREFIX");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> POLICY_REPO_REUSE_CLONE_PATH() {
    return (ConfiEntry<Boolean>) entries().get("POLICY_REPO_REUSE_CLONE_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_MAIN_BRANCH() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_MAIN_BRANCH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_SSH_KEY() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_SSH_KEY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_MANIFEST_PATH() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_MANIFEST_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> POLICY_REPO_CLONE_TIMEOUT() {
    return (ConfiEntry<Integer>) entries().get("POLICY_REPO_CLONE_TIMEOUT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> SCOPES_GIT_FETCH_TIMEOUT() {
    return (ConfiEntry<Double>) entries().get("SCOPES_GIT_FETCH_TIMEOUT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> SCOPES_GIT_MAX_WORKERS() {
    return (ConfiEntry<Integer>) entries().get("SCOPES_GIT_MAX_WORKERS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> SCOPES_GIT_PRELOAD_DRAIN_TIMEOUT() {
    return (ConfiEntry<Double>) entries().get("SCOPES_GIT_PRELOAD_DRAIN_TIMEOUT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> SCOPES_GIT_MAX_ZOMBIES() {
    return (ConfiEntry<Integer>) entries().get("SCOPES_GIT_MAX_ZOMBIES");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> SCOPES_GIT_BACKOFF_BASE_SECONDS() {
    return (ConfiEntry<Double>) entries().get("SCOPES_GIT_BACKOFF_BASE_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> SCOPES_GIT_BACKOFF_MAX_SECONDS() {
    return (ConfiEntry<Double>) entries().get("SCOPES_GIT_BACKOFF_MAX_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> SCOPES_POLICY_CLONE_WAIT_SECONDS() {
    return (ConfiEntry<Double>) entries().get("SCOPES_POLICY_CLONE_WAIT_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> SCOPES_POLICY_CLONE_WAIT_MAX_INFLIGHT() {
    return (ConfiEntry<Integer>) entries().get("SCOPES_POLICY_CLONE_WAIT_MAX_INFLIGHT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LEADER_LOCK_FILE_PATH() {
    return (ConfiEntry<String>) entries().get("LEADER_LOCK_FILE_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<PolicyBundleServerType> POLICY_BUNDLE_SERVER_TYPE() {
    return (ConfiEntry<PolicyBundleServerType>) entries().get("POLICY_BUNDLE_SERVER_TYPE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_BUNDLE_SERVER_TOKEN() {
    return (ConfiEntry<String>) entries().get("POLICY_BUNDLE_SERVER_TOKEN");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_BUNDLE_SERVER_TOKEN_ID() {
    return (ConfiEntry<String>) entries().get("POLICY_BUNDLE_SERVER_TOKEN_ID");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_BUNDLE_SERVER_AWS_REGION() {
    return (ConfiEntry<String>) entries().get("POLICY_BUNDLE_SERVER_AWS_REGION");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_BUNDLE_TMP_PATH() {
    return (ConfiEntry<String>) entries().get("POLICY_BUNDLE_TMP_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_BUNDLE_GIT_ADD_PATTERN() {
    return (ConfiEntry<String>) entries().get("POLICY_BUNDLE_GIT_ADD_PATTERN");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> REPO_WATCHER_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("REPO_WATCHER_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> PUBLISHER_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("PUBLISHER_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> BROADCAST_KEEPALIVE_INTERVAL() {
    return (ConfiEntry<Integer>) entries().get("BROADCAST_KEEPALIVE_INTERVAL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> BROADCAST_KEEPALIVE_TOPIC() {
    return (ConfiEntry<String>) entries().get("BROADCAST_KEEPALIVE_TOPIC");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> MAX_CHANNELS_PER_CLIENT() {
    return (ConfiEntry<Integer>) entries().get("MAX_CHANNELS_PER_CLIENT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STATISTICS_WAKEUP_CHANNEL() {
    return (ConfiEntry<String>) entries().get("STATISTICS_WAKEUP_CHANNEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STATISTICS_STATE_SYNC_CHANNEL() {
    return (ConfiEntry<String>) entries().get("STATISTICS_STATE_SYNC_CHANNEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STATISTICS_SERVER_KEEPALIVE_CHANNEL() {
    return (ConfiEntry<String>) entries().get("STATISTICS_SERVER_KEEPALIVE_CHANNEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STATISTICS_SERVER_KEEPALIVE_TIMEOUT() {
    return (ConfiEntry<String>) entries().get("STATISTICS_SERVER_KEEPALIVE_TIMEOUT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SCOPES_PURGE_CHANNEL() {
    return (ConfiEntry<String>) entries().get("SCOPES_PURGE_CHANNEL");
  }

  /**
   * R216: a deployment still setting the old name gets the port it asked for.
   *
   * <p>{@code SERVER_PORT} was the name before {@code SERVER_BIND_PORT}, and a container platform
   * that links services injects {@code OPAL_SERVER_PORT} with a value like {@code tcp://10.0.0.1}
   * — which is why only a value that is all digits is carried across.
   */
  public void onLoad() {
    String legacy = getString("SERVER_PORT");
    if (legacy != null && !legacy.isEmpty() && legacy.chars().allMatch(Character::isDigit)) {
      SERVER_BIND_PORT().set(Integer.parseInt(legacy));
    }
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> ALL_DATA_TOPIC() {
    return (ConfiEntry<String>) entries().get("ALL_DATA_TOPIC");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> ALL_DATA_ROUTE() {
    return (ConfiEntry<String>) entries().get("ALL_DATA_ROUTE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> ALL_DATA_URL() {
    return (ConfiEntry<String>) entries().get("ALL_DATA_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> DATA_CONFIG_ROUTE() {
    return (ConfiEntry<String>) entries().get("DATA_CONFIG_ROUTE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> DATA_CALLBACK_DEFAULT_ROUTE() {
    return (ConfiEntry<String>) entries().get("DATA_CALLBACK_DEFAULT_ROUTE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<ServerDataSourceConfig> DATA_CONFIG_SOURCES() {
    return (ConfiEntry<ServerDataSourceConfig>) entries().get("DATA_CONFIG_SOURCES");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> DATA_UPDATE_TRIGGER_ROUTE() {
    return (ConfiEntry<String>) entries().get("DATA_UPDATE_TRIGGER_ROUTE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_REPO_WEBHOOK_SECRET() {
    return (ConfiEntry<String>) entries().get("POLICY_REPO_WEBHOOK_SECRET");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> POLICY_REPO_WEBHOOK_ENFORCE_BRANCH() {
    return (ConfiEntry<Boolean>) entries().get("POLICY_REPO_WEBHOOK_ENFORCE_BRANCH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<GitWebhookRequestParams> POLICY_REPO_WEBHOOK_PARAMS() {
    return (ConfiEntry<GitWebhookRequestParams>) entries().get("POLICY_REPO_WEBHOOK_PARAMS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> POLICY_REPO_POLLING_INTERVAL() {
    return (ConfiEntry<Integer>) entries().get("POLICY_REPO_POLLING_INTERVAL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> ALLOWED_ORIGINS() {
    return (ConfiEntry<List<String>>) entries().get("ALLOWED_ORIGINS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> FILTER_FILE_EXTENSIONS() {
    return (ConfiEntry<List<String>>) entries().get("FILTER_FILE_EXTENSIONS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> BUNDLE_IGNORE() {
    return (ConfiEntry<List<String>>) entries().get("BUNDLE_IGNORE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> NO_RPC_LOGS() {
    return (ConfiEntry<Boolean>) entries().get("NO_RPC_LOGS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> SERVER_WORKER_COUNT() {
    return (ConfiEntry<Integer>) entries().get("SERVER_WORKER_COUNT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SERVER_HOST() {
    return (ConfiEntry<String>) entries().get("SERVER_HOST");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SERVER_PORT() {
    return (ConfiEntry<String>) entries().get("SERVER_PORT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> SERVER_BIND_PORT() {
    return (ConfiEntry<Integer>) entries().get("SERVER_BIND_PORT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> ENABLE_DATADOG_APM() {
    return (ConfiEntry<Boolean>) entries().get("ENABLE_DATADOG_APM");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> DEBUG_INTERNAL_STATS() {
    return (ConfiEntry<Boolean>) entries().get("DEBUG_INTERNAL_STATS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> SCOPES() {
    return (ConfiEntry<Boolean>) entries().get("SCOPES");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> SCOPES_REPO_CLONES_SHARDS() {
    return (ConfiEntry<Integer>) entries().get("SCOPES_REPO_CLONES_SHARDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> REDIS_URL() {
    return (ConfiEntry<String>) entries().get("REDIS_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> BASE_DIR() {
    return (ConfiEntry<String>) entries().get("BASE_DIR");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> POLICY_REFRESH_INTERVAL() {
    return (ConfiEntry<Integer>) entries().get("POLICY_REFRESH_INTERVAL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> SCOPES_STORE_READ_TIMEOUT() {
    return (ConfiEntry<Double>) entries().get("SCOPES_STORE_READ_TIMEOUT");
  }

}
