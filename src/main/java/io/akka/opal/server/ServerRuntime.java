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
      broadcaster.setOnGiveUp(this::gracefulShutdown);
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

    this.freezeOnDisconnect = Boolean.TRUE.equals(server.get("BROADCAST_FREEZE_ON_DISCONNECT"));
    this.freezeExemptTopics =
        Set.of(
            common.getString("STATISTICS_ADD_CLIENT_CHANNEL"),
            common.getString("STATISTICS_REMOVE_CLIENT_CHANNEL"),
            server.getString("STATISTICS_WAKEUP_CHANNEL"),
            server.getString("STATISTICS_STATE_SYNC_CHANNEL"),
            server.getString("STATISTICS_SERVER_KEEPALIVE_CHANNEL"),
            server.getString("BROADCAST_KEEPALIVE_TOPIC"),
            POLICY_REPO_WEBHOOK_TOPIC);
    installChannelRestrictions();
    wireInternalSubscriptions();
    if (broadcaster != null) {
      broadcaster.start(this::onBroadcastNotification);
    }
    if (statistics != null) {
      statistics.startKeepalive(server.getString("STATISTICS_SERVER_KEEPALIVE_CHANNEL"));
      // R215: and ask the fleet what it already knows, rather than starting blind.
      statistics.requestFleetState(server.getString("STATISTICS_WAKEUP_CHANNEL"));
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
    return "0.0.0";
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

  private void onInternalNotification(EventNotifier.Subscription subscription, Object data) {
    String topic = subscription.topic();
    if (topic.equals(POLICY_REPO_WEBHOOK_TOPIC)) {
      checkPolicySourceForChanges();
      scopesService.syncAllScopes();
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
    log.error("Broadcaster gave up reconnecting; stopping this process so it can be restarted");
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
    if (broadcaster == null || !Boolean.TRUE.equals(server.get("BROADCAST_HEALTHCHECK_ENABLED"))) {
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
      return;
    }
    log.info("leadership lock acquired, leader pid: {}", ProcessHandle.current().pid());
    if (Boolean.TRUE.equals(server.get("SCOPES"))) {
      startScopePolling();
    }
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
    if (type == PolicySourceTypes.Api) {
      String bundleUrl = server.getString("POLICY_BUNDLE_URL");
      if (bundleUrl == null || bundleUrl.isEmpty()) {
        log.info("no POLICY_BUNDLE_URL configured, the policy watcher is off");
        return;
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
        log.info("no POLICY_REPO_URL configured, the policy watcher is off");
        return;
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
    source.addOnNewPolicyCallback(this::onNewPolicy);
    // R265: a git failure names the remote it could not reach, and a remote can carry a token in
    // its own URL. The message is scrubbed before it reaches a log, whoever wrote it.
    String sourceUrl = server.getString("POLICY_REPO_URL");
    source.addOnFailureCallback(
        exception ->
            log.error(
                "policy source failed: {}",
                io.akka.opal.common.util.Urls.redactUrlInText(exception.toString(), sourceUrl)));
    policySource.set(source);
    source.run();
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

  public BundleMaker bundleMaker(Set<String> directories) {
    Repository repository = repository();
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
        "Publishing data update to topics: {}, reason: {}, entries: {}",
        addressed.topics(),
        update.reason(),
        addressed.update().entries().size());
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
          } catch (RuntimeException e) {
            log.error("periodic scope sync failed: {}", e.toString());
          }
        },
        interval,
        interval,
        java.util.concurrent.TimeUnit.SECONDS);
  }

  private java.util.concurrent.ScheduledExecutorService scopePolling;

  public void shutdown() {
    if (scopePolling != null) {
      scopePolling.shutdownNow();
      scopePolling = null;
    }
    scopesService.stop(
        Double.parseDouble(server.getString("SCOPES_GIT_PRELOAD_DRAIN_TIMEOUT")));
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
