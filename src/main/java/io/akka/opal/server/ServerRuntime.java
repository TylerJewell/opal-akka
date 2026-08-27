package io.akka.opal.server;

import akka.javasdk.client.ComponentClient;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.auth.JwtSigner;
import io.akka.opal.common.auth.Types;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.config.Enums.PolicySourceTypes;
import io.akka.opal.common.git.BundleMaker;
import io.akka.opal.common.git.ClonePathFinder;
import io.akka.opal.common.git.PolicyUpdates;
import io.akka.opal.common.metrics.Metrics;
import io.akka.opal.common.monitoring.Apm;
import io.akka.opal.common.monitoring.Span;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Scopes;
import io.akka.opal.common.sync.NamedLock;
import io.akka.opal.common.sources.ApiPolicySource;
import io.akka.opal.common.sources.GitPolicySource;
import io.akka.opal.common.sources.PolicySource;
import io.akka.opal.common.topics.Topics;
import io.akka.opal.server.config.ServerConfig;
import io.akka.opal.server.pubsub.BroadcastKeepalive;
import io.akka.opal.server.pubsub.Broadcaster;
import io.akka.opal.server.pubsub.Broadcasters;
import io.akka.opal.server.pubsub.ClientTracker;
import io.akka.opal.server.pubsub.ConnectionManager;
import io.akka.opal.server.pubsub.EventNotifier;
import io.akka.opal.server.pubsub.RpcChannel;
import io.akka.opal.server.scopes.GitOps;
import io.akka.opal.server.scopes.ScopeRepository;
import io.akka.opal.server.scopes.ScopesService;
import io.akka.opal.server.stats.OpalStatistics;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the OPAL server is, assembled once.
 *
 * <p>The pieces are the source's own: a policy source, a bundle maker over its clone, a pub/sub
 * broker, a client register, statistics, scopes and — where one is configured — a broadcast
 * backbone. What differs is who owns them: OPAL forks uvicorn workers and elects a leader among
 * them with a file lock, and this runs one instance per node with the watcher driven by a timed
 * component instead (OD-1).
 */
public final class ServerRuntime {

  private static final Logger log = LoggerFactory.getLogger(ServerRuntime.class);

  public static final String POLICY_REPO_WEBHOOK_TOPIC = "webhook";

  private final CommonConfig common;
  private final ServerConfig server;
  private final JwtSigner signer;
  private final EventNotifier notifier = new EventNotifier();
  private final ClientTracker clientTracker = new ClientTracker();
  private final Metrics metrics = new Metrics();
  private final Broadcaster broadcaster;
  private final OpalStatistics statistics;
  private final ScopeRepository scopes;
  private final ScopesService scopesService;
  private final LoadLimiter loadLimiter;

  private final AtomicReference<PolicySource> policySource = new AtomicReference<>();
  private final List<String> connectedTopics = new ArrayList<>();

  private BroadcastKeepalive broadcastKeepalive;

  /** R198: held by the one process per machine that runs the policy watcher. */
  private NamedLock leadershipLock;

  /** R211: the client connections this replica holds, so a resync can close them. */
  private final ConnectionManager connections = new ConnectionManager();

  /** R212: whether a publication made during a backbone gap is dropped. */
  private final boolean freezeOnDisconnect;

  /** R212: the topics a freeze never applies to. */
  private final Set<String> freezeExemptTopics;

  private int frozenInEpisode;
  private int frozenGapGeneration = -1;

  /**
   * R199: whether this server has a publisher at all.
   *
   * <p>Off, the source builds none, and every path that would have used one fails where it
   * reaches for it — the keepalive is never started, and a caller posting a data update gets the
   * answer an unhandled failure gets. Reproduced at the same points rather than turned into a
   * tidier refusal, because a deployment that turned the publisher off and still posts updates is
   * relying on what actually happens.
   */
  public void requirePublisher() {
    if (!Boolean.TRUE.equals(server.get("PUBLISHER_ENABLED"))) {
      throw new IllegalStateException(
          "'NoneType' object has no attribute 'publish_data_updates'");
    }
  }

  /** Whether this process won the lock, which the health route reports. */
  public boolean isLeader() {
    return leadershipLock != null && leadershipLock.isLocked();
  }

  public ServerRuntime(CommonConfig common, ServerConfig server, ComponentClient componentClient) {
    this.common = common;
    this.server = server;
    configureMonitoring(common, server);
    metrics.increment("startup");
    this.signer = buildSigner(common, server);
    this.broadcaster =
        Broadcasters.forUri(
            server.getString("BROADCAST_URI"), server.getString("BROADCAST_CHANNEL_NAME"));
    if (broadcaster != null) {
      broadcaster.configureResilience(
          new Broadcaster.Resilience(
              Boolean.TRUE.equals(server.get("BROADCAST_RECONNECT_ENABLED")),
              (Integer) server.get("BROADCAST_RECONNECT_MAX_RETRIES"),
              Double.parseDouble(server.getString("BROADCAST_RECONNECT_BACKOFF_MIN_SECONDS")),
              Double.parseDouble(server.getString("BROADCAST_RECONNECT_BACKOFF_MAX_SECONDS")),
              (Integer) server.get("BROADCAST_REPLAY_BUFFER_SIZE"),
              Double.parseDouble(server.getString("BROADCAST_RESYNC_SETTLE_SECONDS")),
              Boolean.TRUE.equals(server.get("BROADCAST_RESYNC_ON_RECONNECT"))));
      broadcaster.setOnReconnect(this::resyncAfterBackboneGap);
      // R322: only a reconnecting broadcaster gives up, so only one has the hook. A broadcaster
      // with reconnection turned off ends its session on the first drop, and ending the process
      // for that would turn one backbone blip into a restart the source does not perform.
      if (Boolean.TRUE.equals(server.get("BROADCAST_RECONNECT_ENABLED"))) {
        broadcaster.setOnGiveUp(this::gracefulShutdown);
      }
    }
    this.scopes = new ScopeRepository(componentClient);
    GitOps.configure(
        new GitOps.Settings(
            Double.parseDouble(server.getString("SCOPES_GIT_FETCH_TIMEOUT")),
            (Integer) server.get("SCOPES_GIT_MAX_WORKERS"),
            (Integer) server.get("SCOPES_GIT_MAX_ZOMBIES"),
            Double.parseDouble(server.getString("SCOPES_GIT_BACKOFF_BASE_SECONDS")),
            Double.parseDouble(server.getString("SCOPES_GIT_BACKOFF_MAX_SECONDS"))),
        metrics);
    this.scopesService =
        new ScopesService(
            scopes,
            Path.of(server.getString("BASE_DIR")),
            (Integer) server.get("SCOPES_REPO_CLONES_SHARDS"),
            common.get("POLICY_REPO_POLICY_EXTENSIONS"),
            this::publish,
            server.getString("SCOPES_PURGE_CHANNEL"));
    this.scopesService.configure(metrics, (Integer) server.get("SCOPES_GIT_MAX_WORKERS"));
    this.scopesService.setStoreReadTimeout(
        Double.parseDouble(server.getString("SCOPES_STORE_READ_TIMEOUT")));
    this.statistics =
        Boolean.TRUE.equals(common.get("STATISTICS_ENABLED"))
            ? new OpalStatistics(
                version(),
                workerCount(),
                (Integer) server.get("MAX_CHANNELS_PER_CLIENT"),
                Double.parseDouble(server.getString("STATISTICS_SERVER_KEEPALIVE_TIMEOUT")),
                this::publishObject)
            : null;
    this.loadLimiter = new LoadLimiter(server.getString("CLIENT_LOAD_LIMIT_NOTATION"));

    // R282: a freeze with no resync has no way back. The freeze drops what a client would have
    // been sent during a backbone gap, and the resync on reconnect is the only thing that makes
    // the client ask again — so the source refuses the pair and turns the freeze off rather than
    // dropping updates nothing will replace.
    boolean freezeRequested = Boolean.TRUE.equals(server.get("BROADCAST_FREEZE_ON_DISCONNECT"));
    if (freezeRequested && !Boolean.TRUE.equals(server.get("BROADCAST_RESYNC_ON_RECONNECT"))) {
      log.warn(
          "BROADCAST_FREEZE_ON_DISCONNECT is on with BROADCAST_RESYNC_ON_RECONNECT off; the"
              + " resync is the freeze's only recovery path, so the freeze is disabled");
      freezeRequested = false;
    }
    this.freezeOnDisconnect = freezeRequested;
    this.freezeExemptTopics =
        Set.of(
            common.getString("STATISTICS_ADD_CLIENT_CHANNEL"),
            common.getString("STATISTICS_REMOVE_CLIENT_CHANNEL"),
            server.getString("STATISTICS_WAKEUP_CHANNEL"),
            server.getString("STATISTICS_STATE_SYNC_CHANNEL"),
            server.getString("STATISTICS_SERVER_KEEPALIVE_CHANNEL"),
            server.getString("BROADCAST_KEEPALIVE_TOPIC"),
            POLICY_REPO_WEBHOOK_TOPIC);
    // R327: written once, when the server starts, rather than on the first request for it.
    io.akka.opal.server.api.Jwks.write(this);
    installChannelRestrictions();
    wireInternalSubscriptions();
    if (broadcaster != null) {
      broadcaster.start(this::onBroadcastNotification);
    }
    if (statistics != null) {
      statistics.startKeepalive(server.getString("STATISTICS_SERVER_KEEPALIVE_CHANNEL"));
      // R215: and ask the fleet what it already knows, rather than starting blind.
      statistics.requestFleetState(server.getString("STATISTICS_WAKEUP_CHANNEL"));
      if (broadcaster != null) {
        // R337: fleet statistics are assembled from what arrives on the backbone, so a worker
        // whose reader has ended holds a picture that only goes further out of date. Ending the
        // process is what lets a supervisor put a reading worker in its place.
        broadcaster.setOnReaderEnded(
            () -> gracefulShutdown("the broadcast reader ended and statistics are enabled"));
      }
    }
  }

  /**
   * R285: the keepalive belongs to the leader, not to every process.
   *
   * <p>The source starts it inside the leadership lock, so one worker per machine emits it. Every
   * worker emitting one multiplies the backbone traffic and every peer's delivery by the number
   * of workers, which is a difference a subscriber sees.
   */
  private void startBroadcastKeepalive() {
    if (broadcastKeepalive != null) {
      return;
    }
    broadcastKeepalive =
        BroadcastKeepalive.start(
            broadcaster != null && Boolean.TRUE.equals(server.get("PUBLISHER_ENABLED")),
            (Integer) server.get("BROADCAST_KEEPALIVE_INTERVAL"),
            server.getString("BROADCAST_KEEPALIVE_TOPIC"),
            this::publish);
  }

  /**
   * R159 and R154: the tracer and the statsd destination, set before anything is measured.
   *
   * <p>The source does both in one place at the top of the server's own construction, and in this
   * order — a span opened by a later step would otherwise go nowhere.
   */
  private void configureMonitoring(CommonConfig common, ServerConfig server) {
    Apm.configure(Boolean.TRUE.equals(server.get("ENABLE_DATADOG_APM")), "opal-server");
    String agentHost = System.getenv("DD_AGENT_HOST");
    metrics.configure(
        Boolean.TRUE.equals(common.get("ENABLE_METRICS")),
        agentHost == null || agentHost.isEmpty() ? "localhost" : agentHost,
        8125,
        "opal");
  }

  /** R69: with neither key configured, signing and verification are off entirely. */
  static JwtSigner buildSigner(CommonConfig common, ServerConfig server) {
    return new JwtSigner(
        server.getString("AUTH_PRIVATE_KEY"),
        common.getString("AUTH_PUBLIC_KEY"),
        server.get("AUTH_PRIVATE_KEY_FORMAT"),
        common.get("AUTH_PUBLIC_KEY_FORMAT"),
        server.getString("AUTH_PRIVATE_KEY_PASSPHRASE"),
        common.get("AUTH_JWT_ALGORITHM"),
        common.getString("AUTH_JWT_AUDIENCE"),
        common.getString("AUTH_JWT_ISSUER"));
  }

  public CommonConfig common() {
    return common;
  }

  public ServerConfig config() {
    return server;
  }

  public JwtSigner signer() {
    return signer;
  }

  public EventNotifier notifier() {
    return notifier;
  }

  public ClientTracker clientTracker() {
    return clientTracker;
  }

  public OpalStatistics statistics() {
    return statistics;
  }

  public ScopeRepository scopes() {
    return scopes;
  }

  public ScopesService scopesService() {
    return scopesService;
  }

  public Metrics metrics() {
    return metrics;
  }

  public LoadLimiter loadLimiter() {
    return loadLimiter;
  }

  public boolean scopesEnabled() {
    return Boolean.TRUE.equals(server.get("SCOPES"));
  }

  public String version() {
    return io.akka.opal.common.util.Version.current();
  }

  private int workerCount() {
    String value = System.getenv("UVICORN_NUM_WORKERS");
    if (value == null || !value.matches("\\d+")) {
      return 1;
    }
    return Integer.parseInt(value);
  }

  // -- pub/sub -------------------------------------------------------------

  /**
   * R62 and R63. A token carrying {@code permitted_topics} is confined to them, and the purge
   * channel — plus the sentinel that would receive it — is refused to any external peer, because
   * a forged purge evicts every replica's clone caches at once.
   */
  private void installChannelRestrictions() {
    notifier.addChannelRestriction(
        (topics, channel) -> {
          Object claims = channel.context().get("claims");
          if (!(claims instanceof java.util.Map<?, ?> map) || !map.containsKey("permitted_topics")) {
            return;
          }
          Set<String> permitted =
              new LinkedHashSet<>(
                  io.akka.opal.common.auth.Authz.asStrings(map.get("permitted_topics")));
          List<String> unauthorized = new ArrayList<>();
          for (String topic : topics) {
            if (!permitted.contains(topic)) {
              unauthorized.add(topic);
            }
          }
          if (!unauthorized.isEmpty()) {
            throw new io.akka.opal.common.auth.Unauthorized(
                "Invalid 'topics' to subscribe "
                    + io.akka.opal.common.util.Repr.pySet(unauthorized));
          }
        });
    notifier.addChannelRestriction(
        (topics, channel) -> {
          String purgeChannel = server.getString("SCOPES_PURGE_CHANNEL");
          if (topics.contains(io.akka.opal.server.pubsub.Rpc.ALL_TOPICS)
              || topics.contains(purgeChannel)) {
            throw new io.akka.opal.common.auth.Unauthorized(
                "Topic '"
                    + purgeChannel
                    + "' (and ALL_TOPICS, which would receive it) is server-internal and may not"
                    + " be published or subscribed by external peers");
          }
        });
  }

  /** The server subscribes to its own internal channels, with no channel and so no restriction. */
  private void wireInternalSubscriptions() {
    String serverId = "opal-server-internal";
    List<String> topics = new ArrayList<>();
    topics.add(POLICY_REPO_WEBHOOK_TOPIC);
    topics.add(server.getString("SCOPES_PURGE_CHANNEL"));
    if (statistics != null) {
      topics.add(server.getString("STATISTICS_WAKEUP_CHANNEL"));
      topics.add(server.getString("STATISTICS_STATE_SYNC_CHANNEL"));
      topics.add(server.getString("STATISTICS_SERVER_KEEPALIVE_CHANNEL"));
      topics.add(common.getString("STATISTICS_ADD_CLIENT_CHANNEL"));
      topics.add(common.getString("STATISTICS_REMOVE_CLIENT_CHANNEL"));
    }
    connectedTopics.addAll(topics);
    notifier.subscribe(serverId, topics, this::onInternalNotification, null);
  }

  /** Where a webhook-triggered pull runs, off the thread the notification arrived on. */
  private final java.util.concurrent.ExecutorService webhookTriggers =
      java.util.concurrent.Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "opal-webhook-trigger");
            thread.setDaemon(true);
            return thread;
          });

  private void onInternalNotification(EventNotifier.Subscription subscription, Object data) {
    String topic = subscription.topic();
    if (topic.equals(POLICY_REPO_WEBHOOK_TOPIC)) {
      // R310: the trigger runs off the delivery thread. Deliveries are serial, so a pull that
      // waits on somebody else's git host would otherwise hold up every other subscriber on
      // this publication.
      JsonNode payload = asJson(data);
      webhookTriggers.execute(
          () -> {
            try {
              checkPolicySourceForChanges();
              // R311: a payload naming one scope syncs that scope. Syncing everything instead
              // fetches every tenant's repository because one tenant pushed, and loses the
              // hinted hash the publisher sent with it.
              if (payload != null && payload.isObject() && payload.has("scope_id")) {
                String scopeId = payload.path("scope_id").asText(null);
                if (scopeId == null) {
                  log.warn("Got invalid keyword args for single scope refresh: {}", payload);
                  return;
                }
                boolean forceFetch = payload.path("force_fetch").asBoolean(false);
                String hintedHash =
                    payload.hasNonNull("hinted_hash") ? payload.get("hinted_hash").asText() : null;
                scopesService
                    .findScope(scopeId)
                    .ifPresentOrElse(
                        scope -> scopesService.refreshScope(scope, forceFetch ? null : hintedHash),
                        () -> log.warn("Got a refresh for a scope that is gone: {}", scopeId));
                return;
              }
              scopesService.syncAllScopes();
            } catch (RuntimeException e) {
              log.error("policy watcher trigger failed: {}", e.toString());
            }
          });
      return;
    }
    if (topic.equals(server.getString("SCOPES_PURGE_CHANNEL"))) {
      scopesService.applyPurge(asJson(data));
      return;
    }
    if (statistics == null) {
      return;
    }
    if (topic.equals(server.getString("STATISTICS_WAKEUP_CHANNEL"))) {
      statistics.receiveWakeup(asJson(data), server.getString("STATISTICS_STATE_SYNC_CHANNEL"));
    } else if (topic.equals(server.getString("STATISTICS_STATE_SYNC_CHANNEL"))) {
      statistics.receiveSyncedState(asJson(data));
    } else if (topic.equals(server.getString("STATISTICS_SERVER_KEEPALIVE_CHANNEL"))) {
      statistics.receiveKeepalive(asJson(data));
    } else if (topic.equals(common.getString("STATISTICS_ADD_CLIENT_CHANNEL"))) {
      statistics.addClient(asJson(data));
    } else if (topic.equals(common.getString("STATISTICS_REMOVE_CLIENT_CHANNEL"))) {
      JsonNode node = asJson(data);
      statistics.removeClient(node.isTextual() ? node.asText() : node.path("rpc_id").asText());
    }
  }

  static JsonNode asJson(Object data) {
    if (data instanceof JsonNode node) {
      return node;
    }
    return io.akka.opal.server.pubsub.Rpc.MAPPER.valueToTree(data);
  }

  /**
   * R66 and R212: a publication goes to this replica's subscribers and, where one exists, the
   * backbone — unless the backbone is in a gap, in which case it goes nowhere.
   *
   * <p>The two halves of a publication are independent: local delivery reaches this replica's own
   * clients whether or not the backbone took it. So a change arriving during a gap would be
   * applied by one replica's clients and by nobody else's, and the fleet would disagree for as
   * long as the outage lasted. Dropping it instead leaves every client on the old answer, and the
   * resync after the gap brings all of them to the new one together.
   */
  public void publish(List<String> topics, Object data) {
    boolean inGap = broadcaster != null && broadcaster.isInBackboneGap();
    if (inGap && freezeOnDisconnect && !isFreezeExempt(topics)) {
      int generation = broadcaster.gapGeneration();
      if (frozenInEpisode > 0 && generation != frozenGapGeneration) {
        logEpisodeSummary();
      }
      frozenGapGeneration = generation;
      frozenInEpisode++;
      String message =
          "Broadcaster backbone gap; freezing publish to preserve fleet consistency "
              + "(not delivered to clients; reconciled via resync on reconnect). topics={} "
              + "(suppressed {} so far this gap)";
      if (frozenInEpisode == 1) {
        log.warn(message, topics, frozenInEpisode);
      } else {
        log.debug(message, topics, frozenInEpisode);
      }
      return;
    }
    if (frozenInEpisode > 0 && !inGap) {
      logEpisodeSummary();
    }
    try (Span ignored = Apm.trace("topic_publisher.publish", topics.toString())) {
      notifier.notify(topics, data, null, null);
      if (broadcaster != null) {
        broadcaster.publish(
            new Broadcaster.BroadcastNotification(broadcaster.id(), topics, data));
      }
    }
  }

  /**
   * R212: the topics a freeze does not apply to.
   *
   * <p>Two underscores in front means server-to-server rather than server-to-client — the
   * statistics protocol and the backbone keepalive — and dropping those corrupts state no resync
   * rebuilds: a replica that never hears a peer leave keeps a client that is not there. The
   * configured names of those channels are exempt as well, because a deployment may have renamed
   * them out of the underscore convention, and so is the webhook trigger, which asks this
   * server's own watcher to pull rather than telling a client anything.
   */
  private boolean isFreezeExempt(List<String> topics) {
    if (topics == null || topics.isEmpty()) {
      return false;
    }
    return topics.stream()
        .allMatch(topic -> topic.startsWith("__") || freezeExemptTopics.contains(topic));
  }

  private void logEpisodeSummary() {
    int count = frozenInEpisode;
    frozenInEpisode = 0;
    log.warn(
        "Backbone recovered; froze {} publish(es) during the gap - clients reconcile via the "
            + "reconnect resync",
        count);
  }

  /**
   * R213: after a gap, this replica's own clients are made to re-read everything.
   *
   * <p>Closing their connections is the instruction: a client reconnects on its own and re-runs
   * the whole of its start-up reconciliation, which is what it would have done had it just
   * started. The wait before and after is so a fleet of replicas, all recovering from the same
   * gap, does not close every connection in the fleet at once.
   */
  void resyncAfterBackboneGap() {
    double settle = Double.parseDouble(server.getString("BROADCAST_RESYNC_SETTLE_SECONDS"));
    try {
      if (settle > 0) {
        Thread.sleep(
            (long) (java.util.concurrent.ThreadLocalRandom.current().nextDouble(0, settle) * 1000));
      }
      int closed = connections.closeAllStaggered(0, 0.2);
      log.info("Resync after backbone gap: closed {} client connection(s)", closed);
      if (settle > 0) {
        Thread.sleep((long) (settle * 1000));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * R209: the reader has given up, so this process ends and whatever supervises it starts another.
   *
   * <p>Nothing inside the process can bring a wedged reader back — that is what giving up means —
   * and a process that stays up with a dead reader holds clients that will never hear anything.
   */
  void gracefulShutdown() {
    gracefulShutdown("Broadcaster gave up reconnecting");
  }

  /**
   * Ends this process so whatever supervises it starts another, naming why.
   *
   * <p>The source reaches this from two places — a backbone reader that gave up and a policy
   * watcher that failed — and both send the process a termination signal rather than trying to
   * carry on without the thing that failed.
   */
  void gracefulShutdown(String reason) {
    log.error("{}; stopping this process so it can be restarted", reason);
    new Thread(
            () -> {
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              // Exit rather than halt: the source sends itself a termination signal so its own
              // shutdown lifecycle runs, and halting would skip every shutdown hook this process
              // has — including the one that releases the leadership lock.
              System.exit(1);
            },
            "opal-broadcast-give-up")
        .start();
  }

  /** R214: whether the backbone reader is well enough for this replica to be ready. */
  public boolean isBroadcasterHealthy() {
    // R322: the probe degrades only for a reconnecting broadcaster. One that does not reconnect
    // has no recovery to be waiting on, and reporting it unhealthy takes a worker out of a load
    // balancer for a condition it will never leave.
    if (broadcaster == null
        || !Boolean.TRUE.equals(server.get("BROADCAST_HEALTHCHECK_ENABLED"))
        || !Boolean.TRUE.equals(server.get("BROADCAST_RECONNECT_ENABLED"))) {
      return true;
    }
    boolean healthy = broadcaster.isReaderHealthy();
    metrics.gauge(
        "opal_server.broadcaster_reader_healthy",
        healthy ? 1L : 0L,
        java.util.Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
    return healthy;
  }

  public ConnectionManager connections() {
    return connections;
  }

  public void publish(String topic) {
    publish(List.of(topic), null);
  }

  /** One topic with a payload, which the scope routes use to ask the fleet to sync one scope. */
  public void publish(String topic, Object data) {
    publish(List.of(topic), data);
  }

  private void publishObject(String topic, Object data) {
    publish(List.of(topic), data);
  }

  private void onBroadcastNotification(Broadcaster.BroadcastNotification notification) {
    if (notification.notifier_id() != null
        && notification.notifier_id().equals(broadcaster.id())) {
      return;
    }
    notifier.notify(notification.topics(), notification.data(), null, null);
  }

  // -- the policy source ---------------------------------------------------

  public PolicySource policySource() {
    return policySource.get();
  }

  /**
   * R197 and R198: builds the configured source and takes its first state, if this process is the
   * one that should.
   *
   * <p>Two gates, both the source's. {@code REPO_WATCHER_ENABLED} turns the watcher off entirely,
   * which is how a deployment runs a server that serves bundles and does not pull them. And the
   * watcher runs on exactly one process per machine, decided by a lock on a file: every worker
   * tries, one wins, and the others carry on without a watcher. Without that, one commit becomes
   * one pull and one publication per worker.
   *
   * <p>The lock is taken without waiting, unlike the source's, which blocks a worker's whole
   * start-up until it wins. Blocking here would hold up every other component this process runs,
   * because this process is not one of several identical workers — it is the whole service.
   */
  public synchronized void startPolicySource() {
    if (policySource.get() != null) {
      return;
    }
    if (!Boolean.TRUE.equals(server.get("REPO_WATCHER_ENABLED"))) {
      log.info("REPO_WATCHER_ENABLED is off, the policy watcher will not run");
      return;
    }
    leadershipLock = new NamedLock(server.getString("LEADER_LOCK_FILE_PATH"));
    if (!leadershipLock.tryAcquire()) {
      log.info(
          "another process holds the leadership lock ({}), the policy watcher will not run here",
          server.getString("LEADER_LOCK_FILE_PATH"));
      leadershipLock = null;
      // R288: the source blocks on the lock, so a worker whose leader dies takes over. Blocking
      // here would hold up everything else this process runs, since this process is not one of
      // several identical workers — it is the whole service. Asking again on a timer reaches the
      // same place: the lock a dead leader held is free, and the next ask takes it.
      scheduleLeadershipRetry();
      return;
    }
    log.info("leadership lock acquired, leader pid: {}", ProcessHandle.current().pid());
    if (leadershipRetry != null) {
      leadershipRetry.cancel(false);
      leadershipRetry = null;
    }
    if (Boolean.TRUE.equals(server.get("SCOPES"))) {
      // R286: the leader writes the scope the environment describes before it polls, so a
      // deployment configured the way a single-tenant one is has a scope to serve.
      scopesService.loadScopes(defaultScopeFromConfiguration());
      startScopePolling();
    }
    startBroadcastKeepalive();
    PolicySourceTypes type = server.get("POLICY_SOURCE_TYPE");
    // R190: the clone directory is found rather than assumed, so a run that did not create it
    // still reads the one that is there, and a run that did leaves no earlier clone behind.
    String clonePath =
        new ClonePathFinder(
                server.getString("POLICY_REPO_CLONE_PATH"),
                server.getString("POLICY_REPO_CLONE_FOLDER_PREFIX"),
                Boolean.TRUE.equals(server.get("POLICY_REPO_REUSE_CLONE_PATH")))
            .clonePathOrNew()
            .toString();
    log.info("Policy repo will be cloned to: {}", clonePath);
    PolicySource source;
    try {
    if (type == PolicySourceTypes.Api) {
      String bundleUrl = server.getString("POLICY_BUNDLE_URL");
      // R283: an unset url is a misconfiguration rather than a way to turn the watcher off —
      // REPO_WATCHER_ENABLED is that. The source warns and builds the source anyway, whose first
      // run fails and ends the process, so a deployment that forgot the url is told loudly
      // rather than serving no policy quietly.
      if (bundleUrl == null || bundleUrl.isEmpty()) {
        log.warn("POLICY_BUNDLE_URL is unset but policy watcher is enabled! disabling watcher.");
      }
      source =
          new ApiPolicySource(
              bundleUrl,
              clonePath,
              (Integer) server.get("POLICY_REPO_POLLING_INTERVAL"),
              server.getString("POLICY_BUNDLE_SERVER_TOKEN"),
              server.getString("POLICY_BUNDLE_SERVER_TOKEN_ID"),
              server.getString("POLICY_BUNDLE_SERVER_AWS_REGION"),
              server.get("POLICY_BUNDLE_SERVER_TYPE"),
              server.getString("POLICY_BUNDLE_TMP_PATH"),
              server.getString("POLICY_BUNDLE_GIT_ADD_PATTERN"));
    } else {
      String repoUrl = server.getString("POLICY_REPO_URL");
      if (repoUrl == null || repoUrl.isEmpty()) {
        log.warn("POLICY_REPO_URL is unset but repo watcher is enabled! disabling watcher.");
      }
      source =
          new GitPolicySource(
              repoUrl,
              clonePath,
              server.getString("POLICY_REPO_MAIN_BRANCH"),
              server.getString("POLICY_REPO_SSH_KEY"),
              (Integer) server.get("POLICY_REPO_POLLING_INTERVAL"),
              (Integer) server.get("POLICY_REPO_CLONE_TIMEOUT"));
    }
    } catch (RuntimeException e) {
      // R283: the source builds its watcher inside the leadership lock, so a watcher that cannot
      // be built at all leaves the lock released and the rest of the server serving — the clone
      // on disk still answers the bundle routes, and nothing else this process does depends on
      // the watcher.
      log.error("could not start the policy watcher: {}", e.getMessage());
      releaseLeadership();
      return;
    }
    source.addOnNewPolicyCallback(this::onNewPolicy);
    // R265: a git failure names the remote it could not reach, and a remote can carry a token in
    // its own URL. The message is scrubbed before it reaches a log, whoever wrote it.
    String sourceUrl = server.getString("POLICY_REPO_URL");
    source.addOnFailureCallback(
        exception -> {
          log.error(
              "policy watcher failed with exception: {}",
              io.akka.opal.common.util.Urls.redactUrlInText(exception.toString(), sourceUrl));
          // R284: the watcher is not something the process can run without. A server whose clone
          // permanently fails would otherwise stay up with a dead watcher and no signal to
          // whatever supervises it.
          policySource.set(null);
          gracefulShutdown("The policy watcher failed");
        });
    policySource.set(source);
    // R321: on a thread of its own. The first fetch is retried until it succeeds, so running it
    // here would hold up the rest of the service's start-up for as long as a repository or a
    // bundle server is unreachable — which the source does not, because its watcher is a task
    // beside the application rather than a step inside its construction.
    Thread start = new Thread(source::run, "opal-policy-source-start");
    start.setDaemon(true);
    start.start();
  }

  /**
   * R286: the scope OPAL's own single-repository configuration describes, or null when there is
   * no repository url to build one from.
   *
   * <p>Its id is {@code default}, which is the id the bundle route falls back to when a caller
   * names a scope that does not exist.
   */
  Scopes.Scope defaultScopeFromConfiguration() {
    String repoUrl = server.getString("POLICY_REPO_URL");
    if (repoUrl == null || repoUrl.isEmpty()) {
      return null;
    }
    io.akka.opal.common.schemas.PolicySource.AuthData auth =
        io.akka.opal.common.schemas.PolicySource.NoAuthData.get();
    String sshKey = server.getString("POLICY_REPO_SSH_KEY");
    if (sshKey != null && !sshKey.isEmpty()) {
      String privateKey = io.akka.opal.common.confi.Keys.maybeDecodeMultiline(sshKey);
      auth = new io.akka.opal.common.schemas.PolicySource.SSHAuthData(
          "ssh", "git", null, privateKey);
    }
    PolicySourceTypes type = server.get("POLICY_SOURCE_TYPE");
    return new Scopes.Scope(
        "default",
        new io.akka.opal.common.schemas.PolicySource.GitPolicyScopeSource(
            type.name().toLowerCase(java.util.Locale.ROOT),
            repoUrl,
            auth,
            null,
            null,
            null,
            server.getString("POLICY_REPO_MANIFEST_PATH"),
            null,
            server.getString("POLICY_REPO_MAIN_BRANCH")),
        null);
  }

  /** Where the retries that wait for the leadership lock run. */
  private final java.util.concurrent.ScheduledExecutorService leadership =
      java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "opal-leadership");
            thread.setDaemon(true);
            return thread;
          });

  private volatile java.util.concurrent.ScheduledFuture<?> leadershipRetry;

  private void scheduleLeadershipRetry() {
    if (leadershipRetry != null) {
      return;
    }
    // Not a configuration entry: the source has none, because it blocks on the lock instead.
    long seconds = 5;
    leadershipRetry =
        leadership.scheduleWithFixedDelay(
            () -> {
              try {
                startPolicySource();
              } catch (RuntimeException e) {
                log.error("could not take leadership: {}", e.toString());
              }
            },
            seconds,
            seconds,
            java.util.concurrent.TimeUnit.SECONDS);
  }

  /** Gives the leadership lock up, so another process can take the watcher on. */
  private void releaseLeadership() {
    if (leadershipLock != null) {
      leadershipLock.close();
      leadershipLock = null;
    }
  }

  public void checkPolicySourceForChanges() {
    PolicySource source = policySource.get();
    if (source == null) {
      return;
    }
    try {
      source.checkForChanges();
    } catch (Exception e) {
      // R265: the message names the remote, and a remote can carry a credential in its own URL.
      log.error(
          "policy check failed: {}",
          io.akka.opal.common.util.Urls.redactUrlInText(
              e.toString(), server.getString("POLICY_REPO_URL")));
    }
  }

  /** R38, R39: publish what changed, on the policy topics the directories map to. */
  void onNewPolicy(ObjectId oldCommit, ObjectId newCommit) {
    Repository repository = repository();
    if (repository == null) {
      return;
    }
    try {
      Policy.PolicyUpdateMessageNotification notification =
          PolicyUpdates.createPolicyUpdate(
              repository,
              oldCommit,
              newCommit,
              server.get("FILTER_FILE_EXTENSIONS"),
              server.get("BUNDLE_IGNORE"));
      if (notification == null) {
        return;
      }
      publish(notification.topics(), notification.update());
    } catch (Exception e) {
      log.error(
          "could not publish a policy update: {}",
          io.akka.opal.common.util.Urls.redactUrlInText(e.toString(), null));
    }
  }

  /** The shape the source logs each published entry in. */
  private static List<java.util.Map<String, Object>> loggedEntries(
      List<Data.DataSourceEntry> entries) {
    List<java.util.Map<String, Object>> logged = new ArrayList<>();
    if (entries == null) {
      return logged;
    }
    for (Data.DataSourceEntry entry : entries) {
      java.util.LinkedHashMap<String, Object> row = new java.util.LinkedHashMap<>();
      row.put("url", io.akka.opal.common.util.Urls.redactUrl(entry.url()));
      row.put("method", entry.save_method());
      row.put("path", entry.dst_path() == null || entry.dst_path().isEmpty() ? "/" : entry.dst_path());
      row.put("inline_data", entry.data() != null);
      row.put("topics", entry.topics());
      logged.add(row);
    }
    return logged;
  }

  public Repository repository() {
    PolicySource source = policySource.get();
    if (source instanceof GitPolicySource git) {
      return git.repository();
    }
    if (source instanceof ApiPolicySource api) {
      return api.repository();
    }
    return null;
  }

  /**
   * R281: the clone on disk, whether or not this process is the one that pulls into it.
   *
   * <p>The bundle routes resolve the clone directory per request rather than reading the running
   * watcher's handle. Two deployments depend on that: one with {@code REPO_WATCHER_ENABLED} off,
   * which serves bundles and pulls nothing, and every process that lost the leadership lock —
   * both have a clone on disk and no source object, and both are expected to answer.
   *
   * <p>Returns null when there is no clone directory or it holds no {@code .git}, which the
   * routes report as {@code policy repo was not found}.
   */
  public Repository repositoryOnDisk() {
    Repository running = repository();
    if (running != null) {
      return running;
    }
    java.nio.file.Path clonePath =
        new ClonePathFinder(
                server.getString("POLICY_REPO_CLONE_PATH"),
                server.getString("POLICY_REPO_CLONE_FOLDER_PREFIX"),
                Boolean.TRUE.equals(server.get("POLICY_REPO_REUSE_CLONE_PATH")))
            .clonePath();
    if (clonePath == null || !java.nio.file.Files.exists(clonePath.resolve(".git"))) {
      return null;
    }
    try {
      return new org.eclipse.jgit.storage.file.FileRepositoryBuilder()
          .setGitDir(clonePath.resolve(".git").toFile())
          .readEnvironment()
          .build();
    } catch (Exception e) {
      log.error("could not open the policy clone at {}: {}", clonePath, e.toString());
      return null;
    }
  }

  public BundleMaker bundleMaker(Set<String> directories) {
    return bundleMaker(repositoryOnDisk(), directories);
  }

  public BundleMaker bundleMaker(Repository repository, Set<String> directories) {
    if (repository == null) {
      return null;
    }
    return new BundleMaker(
        repository,
        directories,
        server.get("FILTER_FILE_EXTENSIONS"),
        server.getString("POLICY_REPO_MANIFEST_PATH"),
        server.get("BUNDLE_IGNORE"),
        common.get("POLICY_REPO_POLICY_EXTENSIONS"));
  }

  // -- data ----------------------------------------------------------------

  /** R22: expand every entry's topics, rewrite them onto the entry, and publish the union. */
  /** Where an update is addressed, and the entries as they go on the wire. */
  public record Addressed(List<String> topics, Data.DataUpdate update) {}

  /**
   * R22: the union of every entry's expansion, and every entry's own topics rewritten to its
   * expansion.
   *
   * <p>The two halves are separable and doing only the first is a silent no-op: the update
   * reaches a client subscribed to an ancestor, that client matches the entry's topics against
   * what it subscribed to by string equality, finds nothing, and reports success having written
   * nothing.
   *
   * <p>Apart from the publish so the addressing can be put a question without a cluster around
   * it.
   */
  public static Addressed addressDataUpdate(Data.DataUpdate update, String scopePrefix) {
    Set<String> allTopicCombos = new LinkedHashSet<>();
    List<Data.DataSourceEntry> rewritten = new ArrayList<>();
    for (Data.DataSourceEntry entry : update.entries()) {
      if (entry.topics() == null || entry.topics().isEmpty()) {
        log.warn(
            "No topics were provided for the entry with url: {}",
            io.akka.opal.common.util.Urls.redactUrl(entry.url()));
        rewritten.add(entry);
        continue;
      }
      List<String> combos = new ArrayList<>();
      for (String topic : entry.topics()) {
        combos.addAll(Topics.topicCombos(topic));
      }
      rewritten.add(entry.withTopics(combos));
      allTopicCombos.addAll(combos);
    }
    List<String> topics = new ArrayList<>();
    for (String topic : allTopicCombos) {
      topics.add(scopePrefix == null ? topic : scopePrefix + ":" + topic);
    }
    return new Addressed(
        topics, new Data.DataUpdate(update.id(), rewritten, update.reason(), update.callback()));
  }

  public void publishDataUpdate(Data.DataUpdate update, String scopePrefix) {
    Addressed addressed = addressDataUpdate(update, scopePrefix);
    log.info(
        "[{}] Publishing data update to topics: {}, reason: {}, entries: {}",
        ProcessHandle.current().pid(),
        addressed.topics(),
        update.reason(),
        // R329: the entries themselves, one map each, with the url redacted. A count says a
        // publication happened; this says which sources it names, which is what an operator
        // reading the log after a fleet-wide refetch is looking for.
        loggedEntries(addressed.update().entries()));
    publish(addressed.topics(), addressed.update());
  }

  /**
   * R241: the leader syncs every scope that asks to be polled, on a timer.
   *
   * <p>Nothing else does. A scope's repository can gain a commit with no webhook behind it and no
   * client asking, and without this pass that commit is invisible until somebody restarts the
   * server. It runs on the leader alone for the same reason the watcher does: N replicas polling
   * the same hundred repositories is N times the load on somebody else's git host.
   */
  private void startScopePolling() {
    int interval = (Integer) server.get("POLICY_REFRESH_INTERVAL");
    scopesService.preloadScopes(
        Double.parseDouble(server.getString("SCOPES_GIT_PRELOAD_DRAIN_TIMEOUT")));
    if (interval <= 0) {
      log.info("POLICY_REFRESH_INTERVAL is not positive, scopes will not be polled");
      return;
    }
    scopePolling = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
        runnable -> {
          Thread thread = new Thread(runnable, "opal-scope-poll");
          thread.setDaemon(true);
          return thread;
        });
    scopePolling.scheduleWithFixedDelay(
        () -> {
          try {
            metrics.gauge("opal_server.scopes.leader", 1L, null);
            log.info("Periodic sync");
            scopesService.syncAllScopes(true, true);
          } catch (Throwable e) {
            // R317: anything at all, because a scheduled task that lets a throwable out is never
            // scheduled again. Scope syncing would stop for the life of the process with the
            // pods still ready and the health route still answering 200.
            log.error("Periodic sync (sync_scopes) failed", e);
          }
        },
        interval,
        interval,
        java.util.concurrent.TimeUnit.SECONDS);
  }

  private java.util.concurrent.ScheduledExecutorService scopePolling;

  /**
   * R336: the ceiling on the shutdown drain of the delete floor's clone purges.
   *
   * <p>Its own number rather than the preload's: shutdown runs while an orchestrator's grace
   * period is counting down, so a longer block converts a clean exit into a kill. A purge's first
   * act is to take the source lock, which a sync can hold across a whole clone.
   */
  static final double SCOPES_DRAIN_TIMEOUT_SECONDS = 5.0;

  /** How long the shutdown waits for a publish already under way. */
  static final double PUBLISH_DRAIN_TIMEOUT_SECONDS = 5.0;

  public void shutdown() {
    leadership.shutdownNow();
    // R385: a pull that is publishing when shutdown arrives finishes publishing. Cancelling it
    // leaves a repository advanced on disk and a fleet that was never told, which nothing later
    // repairs — the next poll finds no change because the change is already in the clone.
    webhookTriggers.shutdown();
    try {
      if (!webhookTriggers.awaitTermination(
          (long) (PUBLISH_DRAIN_TIMEOUT_SECONDS * 1000), java.util.concurrent.TimeUnit.MILLISECONDS)) {
        log.warn("a publish was still in flight when the shutdown drain gave up waiting");
        webhookTriggers.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      webhookTriggers.shutdownNow();
    }
    if (scopePolling != null) {
      scopePolling.shutdownNow();
      scopePolling = null;
    }
    scopesService.stop(SCOPES_DRAIN_TIMEOUT_SECONDS);
    if (leadershipLock != null) {
      leadershipLock.release();
      leadershipLock = null;
    }
    if (statistics != null) {
      statistics.stop();
    }
    PolicySource source = policySource.get();
    if (source != null) {
      source.stop();
    }
    if (broadcastKeepalive != null) {
      broadcastKeepalive.close();
      broadcastKeepalive = null;
    }
    if (broadcaster != null) {
      broadcaster.close();
    }
  }

  /** Exposed so a websocket handler can register the channel's claims for the restrictions. */
  public void attachClaims(RpcChannel channel, java.util.Map<String, Object> claims) {
    channel.context().put("claims", claims);
  }

  /** The pem/ssh/der choice the public key was read with, for the JWKS route. */
  public Types.EncryptionKeyFormat publicKeyFormat() {
    return common.get("AUTH_PUBLIC_KEY_FORMAT");
  }
}
