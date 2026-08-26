package io.akka.opal.client;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.client.data.DataFetcher;
import io.akka.opal.client.data.DataUpdater;
import io.akka.opal.client.engine.CedarRunner;
import io.akka.opal.client.engine.EngineRunner;
import io.akka.opal.client.engine.OpaRunner;
import io.akka.opal.client.policy.PolicyFetcher;
import io.akka.opal.client.policy.PolicyUpdater;
import io.akka.opal.client.pubsub.PubSubClient;
import io.akka.opal.client.store.CedarClient;
import io.akka.opal.client.store.LivenessProbe;
import io.akka.opal.client.store.OpaClient;
import io.akka.opal.client.store.PolicyStoreClient;
import io.akka.opal.client.store.PolicyStoreClientFactory;
import io.akka.opal.common.auth.JwtVerifier;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.config.Enums.PolicyStoreTypes;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.topics.Topics;
import io.akka.opal.server.pubsub.Rpc;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the OPAL client is, assembled once.
 *
 * <p>The client's whole job is to keep one policy engine in step with a server it does not
 * control: subscribe, fetch what changed, write it in, and report that it did. The parts that
 * look incidental — the transaction log, the liveness probe, the backup — are what let it answer
 * "am I current" without asking the server, which is the question a load balancer in front of a
 * policy engine actually needs answered.
 */
public final class ClientRuntime implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ClientRuntime.class);

  private final CommonConfig common;
  private final ClientConfig config;
  private final PolicyStoreClient store;
  private final JwtVerifier verifier;
  private final CallbacksRegister callbacksRegister;
  private final CallbacksReporter callbacksReporter;
  private final DataFetcher dataFetcher;
  private final DataUpdater dataUpdater;
  private final PolicyUpdater policyUpdater;
  private final List<String> policyTopics;
  private final List<String> dataTopics;
  private final String opalClientId;
  private final boolean offlineModeEnabled;
  private final Path storeBackupPath;
  private final int storeBackupInterval;

  private volatile boolean connectivityDisabled;
  private volatile boolean backupLoaded;

  /**
   * R251: which shard this client belongs to, announced on the pub/sub connection.
   *
   * <p>Not a configuration entry on either side: the source takes it as an argument to the object
   * that builds the client, so a deployment that shards sets it in code. Unset, no header is sent.
   */
  private volatile String shardId;

  public void setShardId(String value) {
    this.shardId = value;
  }
  private EngineRunner engineRunner;
  private LivenessProbe livenessProbe;
  private PubSubClient policyChannel;
  private PubSubClient dataChannel;
  private ScheduledExecutorService backupScheduler;
  private ScheduledFuture<?> backupTask;

  public ClientRuntime(CommonConfig common, ClientConfig config) {
    this.common = common;
    this.config = config;
    PolicyStoreTypes storeType = config.get("POLICY_STORE_TYPE");
    boolean inlineOpaEnabled = Boolean.TRUE.equals(config.get("INLINE_OPA_ENABLED"));

    boolean offline = Boolean.TRUE.equals(config.get("OFFLINE_MODE_ENABLED"));
    // R101: offline mode needs a store this process can restore into, which an external
    // engine is not — the backup would be written and never read back.
    if (offline && !inlineOpaEnabled) {
      log.warn(
          "Offline mode was enabled, but isn't supported when using an external policy store"
              + " (inline OPA is disabled)");
      offline = false;
    }
    this.offlineModeEnabled = offline;
    this.connectivityDisabled =
        offlineModeEnabled
            && Boolean.TRUE.equals(config.get("DEFAULT_OPAL_SERVER_CONNECTIVITY_DISABLED"));

    this.store = PolicyStoreClientFactory.create(config, true);
    this.verifier =
        new JwtVerifier(
            common.getString("AUTH_PUBLIC_KEY"),
            common.get("AUTH_PUBLIC_KEY_FORMAT"),
            common.get("AUTH_JWT_ALGORITHM"),
            common.getString("AUTH_JWT_AUDIENCE"),
            common.getString("AUTH_JWT_ISSUER"));

    Data.UpdateCallback defaultCallbacks = config.get("DEFAULT_UPDATE_CALLBACKS");
    this.callbacksRegister =
        new CallbacksRegister(
            defaultCallbacks == null ? List.of() : defaultCallbacks.callbacks(),
            config.get("DEFAULT_UPDATE_CALLBACK_CONFIG"));
    this.dataFetcher =
        new DataFetcher(
            new io.akka.opal.common.fetcher.FetchingEngine(
                new io.akka.opal.common.fetcher.FetcherRegister(
                    io.akka.opal.common.util.Http.forClient(),
                    (Double) common.get("HTTP_FETCHER_TIMEOUT"),
                    common.get("FETCH_PROVIDER_MODULES")),
                (Integer) common.get("FETCHING_WORKER_COUNT"),
                (Integer) common.get("FETCHING_CALLBACK_TIMEOUT"),
                (Integer) common.get("FETCHING_ENQUEUE_TIMEOUT"))
                .withDefaultRetry(retryPolicy(config.get("DATA_UPDATER_CONN_RETRY"))))
            .withDefaultDataUrl(config.getString("DEFAULT_DATA_URL"));
    this.callbacksReporter = new CallbacksReporter(callbacksRegister, dataFetcher);

    this.dataTopics = scopedDataTopics();
    this.policyTopics = scopedPolicyTopics();
    this.opalClientId =
        config.getString("OPAL_CLIENT_STAT_ID") == null
            ? "CLIENT_" + java.util.UUID.randomUUID().toString().replace("-", "")
            : config.getString("OPAL_CLIENT_STAT_ID");

    this.dataUpdater =
        new DataUpdater(
            store,
            dataFetcher,
            callbacksRegister,
            callbacksReporter,
            dataTopics,
            Boolean.TRUE.equals(config.get("SHOULD_REPORT_ON_DATA_UPDATES")),
            Boolean.TRUE.equals(config.get("SPLIT_ROOT_DATA")));
    this.policyUpdater =
        new PolicyUpdater(
            store,
            new PolicyFetcher(
                config.getString("SERVER_URL"),
                config.getString("CLIENT_TOKEN"),
                config.getString("SCOPE_ID"),
                config.get("POLICY_UPDATER_CONN_RETRY")),
            callbacksReporter,
            config.get("POLICY_SUBSCRIPTION_DIRS"),
            Boolean.TRUE.equals(config.get("SHOULD_REPORT_ON_DATA_UPDATES")));

    this.storeBackupPath = Path.of(config.getString("STORE_BACKUP_PATH"));
    this.storeBackupInterval = (Integer) config.get("STORE_BACKUP_INTERVAL");
  }

  /** R20: a scoped client subscribes by scope id rather than by directory. */
  private List<String> scopedPolicyTopics() {
    String scopeId = config.getString("SCOPE_ID");
    if ("default".equals(scopeId)) {
      return Topics.pubsubTopicsFromDirectories(config.get("POLICY_SUBSCRIPTION_DIRS"));
    }
    return List.of(scopeId + ":policy:.");
  }

  private List<String> scopedDataTopics() {
    String scopeId = config.getString("SCOPE_ID");
    List<String> topics = config.get("DATA_TOPICS");
    if ("default".equals(scopeId)) {
      return topics;
    }
    List<String> scoped = new ArrayList<>();
    for (String topic : topics) {
      scoped.add(scopeId + ":data:" + topic);
    }
    return scoped;
  }

  public ClientConfig config() {
    return config;
  }

  public PolicyStoreClient store() {
    return store;
  }

  public JwtVerifier verifier() {
    return verifier;
  }

  public CallbacksRegister callbacksRegister() {
    return callbacksRegister;
  }

  public PolicyUpdater policyUpdater() {
    return policyUpdater;
  }

  public DataUpdater dataUpdater() {
    return dataUpdater;
  }

  public boolean offlineModeEnabled() {
    return offlineModeEnabled;
  }

  public boolean connectivityDisabled() {
    return connectivityDisabled;
  }

  public List<String> policyTopics() {
    return policyTopics;
  }

  public List<String> dataTopics() {
    return dataTopics;
  }

  // -- lifecycle -----------------------------------------------------------

  public void start() {
    // R246: a client told not to talk to the server, with a backup on disk to start from, has
    // nothing to wait for. Waiting anyway means blocking start-up on a server this deployment
    // was configured never to reach.
    boolean haveBackup = offlineModeEnabled && Files.isRegularFile(storeBackupPath);
    // R95: wait for the server to admit us before starting anything at all.
    if (Boolean.TRUE.equals(config.get("WAIT_ON_SERVER_LOAD"))
        && !(connectivityDisabled && haveBackup)) {
      waitForServerLoadLimit();
    }
    startEngineIfInline();
    if (offlineModeEnabled) {
      loadStoreFromBackup();
      startPeriodicBackup();
    }
    // R247: offline with nothing to be offline from is worse than being online. A client that
    // could not load a backup has an empty store and no way to fill it, so it connects after all.
    if (connectivityDisabled && !backupLoaded) {
      log.warn(
          "OPAL server connectivity is disabled but no backup could be loaded; "
              + "falling back to server connection");
      connectivityDisabled = false;
    }
    maybeInitHealthcheckPolicy();
    startLivenessProbe();
    if (!connectivityDisabled) {
      startUpdaters();
    }
  }

  /** R95: {@code GET /loadlimit} until it answers 200, with random exponential backoff. */
  void waitForServerLoadLimit() {
    String url = config.getString("SERVER_URL") + "/loadlimit";
    ConnRetryOptions retry = config.get("POLICY_UPDATER_CONN_RETRY");
    for (int attempt = 1; ; attempt++) {
      try {
        // R250: the route is behind the same authenticator as every other server route, so a
        // probe with no token gets a 401 for ever and the client never starts.
        HttpRequest.Builder request =
            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET();
        String token = config.getString("CLIENT_TOKEN");
        if (token != null && !token.isEmpty()) {
          request.header("Authorization", "Bearer " + token);
        }
        HttpResponse<Void> response =
            io.akka.opal.common.util.Http.forClient()
                .send(request.build(), HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() == 200) {
          return;
        }
        log.info("waiting on server load limit, got {}", response.statusCode());
      } catch (Exception e) {
        log.info("waiting on server load limit: {}", e.toString());
      }
      try {
        Thread.sleep(retry.waitMillis(Math.min(attempt, 10), ThreadLocalRandom.current()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** R96: OPA or Cedar as a child process, when the store type matches and inline is asked for. */
  EngineRunner buildEngineRunner() {
    PolicyStoreTypes storeType = config.get("POLICY_STORE_TYPE");
    ConnRetryOptions retry = config.get("POLICY_STORE_CONN_RETRY");
    if (storeType == PolicyStoreTypes.OPA
        && Boolean.TRUE.equals(config.get("INLINE_OPA_ENABLED"))) {
      return new OpaRunner(
          config.get("INLINE_OPA_CONFIG"),
          config.get("INLINE_OPA_LOG_FORMAT"),
          config.getString("INLINE_OPA_EXEC_PATH"),
          config.getString("POLICY_STORE_URL"),
          retry.wait_time(),
          Boolean.TRUE.equals(config.get("OPA_V0_COMPAT")));
    }
    if (storeType == PolicyStoreTypes.CEDAR
        && Boolean.TRUE.equals(config.get("INLINE_CEDAR_ENABLED"))) {
      return new CedarRunner(
          config.get("INLINE_CEDAR_CONFIG"),
          config.get("INLINE_CEDAR_LOG_FORMAT"),
          config.getString("INLINE_CEDAR_EXEC_PATH"),
          config.getString("POLICY_STORE_URL"),
          retry.wait_time());
    }
    return null;
  }

  void startEngineIfInline() {
    engineRunner = buildEngineRunner();
    if (engineRunner == null) {
      return;
    }
    List<Runnable> rehydration = new ArrayList<>();
    if (offlineModeEnabled) {
      rehydration.add(this::loadStoreFromBackup);
    }
    rehydration.add(
        () -> {
          if (!connectivityDisabled) {
            policyUpdater.triggerUpdatePolicy(null, true);
          }
        });
    rehydration.add(
        () -> {
          if (!connectivityDisabled) {
            fetchBaseDataConfiguration("Policy store restart");
          }
        });
    engineRunner.registerRestartCallbacks(rehydration);
    // OD-5: the rest of start-up waits here for as long as it takes. An engine the client is
    // responsible for running is not optional — a client that carried on without it would report
    // itself unready while answering nothing, and the launch is retried with backoff until it
    // succeeds, so a binary that appears later is picked up.
    engineRunner.start();
    engineRunner.waitUntilReady();
  }

  void startLivenessProbe() {
    if (!Boolean.TRUE.equals(config.get("POLICY_STORE_LIVENESS_PROBE_ENABLED"))) {
      log.info("policy store liveness probe disabled via POLICY_STORE_LIVENESS_PROBE_ENABLED");
      return;
    }
    int timeout = (Integer) config.get("POLICY_STORE_LIVENESS_PROBE_TIMEOUT_SECONDS");
    int interval = (Integer) config.get("POLICY_STORE_LIVENESS_PROBE_INTERVAL_SECONDS");
    if (store instanceof OpaClient opa) {
      livenessProbe =
          new LivenessProbe(
              "OPA",
              opa.baseUrl() + "/health",
              opa.http(),
              timeout,
              interval,
              opa.state()::setEngineReachable,
              opa.state()::engineReachable);
      livenessProbe.start();
    } else if (store instanceof CedarClient cedar) {
      livenessProbe =
          new LivenessProbe(
              "Cedar",
              cedar.apiUrl() + "/",
              cedar.http(),
              timeout,
              interval,
              cedar::setEngineReachable,
              cedar::engineReachable);
      livenessProbe.start();
    }
  }

  /** R90: the transaction log becomes a policy inside the engine, when asked for. */
  void maybeInitHealthcheckPolicy() {
    if (!Boolean.TRUE.equals(config.get("OPA_HEALTH_CHECK_POLICY_ENABLED"))) {
      return;
    }
    try {
      String template = healthcheckPolicyTemplate();
      store.initHealthcheckPolicy(ClientConfig.OPA_HEALTH_CHECK_POLICY_PATH, template);
    } catch (Exception e) {
      log.error("healthcheck policy enabled but could not be initialized!", e);
    }
  }

  static String healthcheckPolicyTemplate() {
    try (var in =
        ClientRuntime.class.getResourceAsStream("/engine/healthcheck/opal.rego")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException("could not read the healthcheck policy template", e);
    }
  }

  /** R92: on connect and on every reconnect, re-trigger policy and re-read the whole data set. */
  public synchronized void startUpdaters() {
    if (policyChannel != null || dataChannel != null) {
      return;
    }
    String pubsubUrl = config.getString("SERVER_PUBSUB_URL");
    String token = config.getString("CLIENT_TOKEN");
    int keepAlive = (Integer) config.get("KEEP_ALIVE_INTERVAL");

    policyChannel =
        new PubSubClient(
            pubsubUrl,
            policyTopics,
            token,
            this::onPolicyNotification,
            client -> {
              policyUpdater.triggerUpdatePolicy(null, false);
              publishStatistics(client, policyTopics);
            },
            () -> {},
            keepAlive)
            .withShardId(shardId);
    dataChannel =
        new PubSubClient(
            pubsubUrl,
            dataTopics,
            token,
            this::onDataNotification,
            client -> {
              fetchBaseDataConfiguration("Initial load");
              publishStatistics(client, dataTopics);
            },
            () -> {},
            keepAlive)
            .withShardId(shardId);
    if (Boolean.TRUE.equals(config.get("POLICY_UPDATER_ENABLED"))) {
      policyChannel.start();
    } else {
      policyChannel = null;
    }
    if (Boolean.TRUE.equals(config.get("DATA_UPDATER_ENABLED"))) {
      dataChannel.start();
    } else {
      dataChannel = null;
    }
  }

  private void publishStatistics(PubSubClient client, List<String> topics) {
    if (!Boolean.TRUE.equals(common.get("STATISTICS_ENABLED"))) {
      return;
    }
    client.waitUntilReady(Duration.ofSeconds(5));
    Map<String, Object> message = new LinkedHashMap<>();
    message.put("topics", topics);
    message.put("client_id", opalClientId);
    message.put("rpc_id", client.channelId());
    client.publish(List.of(common.getString("STATISTICS_ADD_CLIENT_CHANNEL")), message);
  }

  void onPolicyNotification(String topic, JsonNode data) {
    if (data == null || data.isNull()) {
      log.warn("got policy update message without data, skipping policy update!");
      return;
    }
    try {
      policyUpdater.onPolicyUpdateMessage(
          Rpc.MAPPER.treeToValue(data, Policy.PolicyUpdateMessage.class));
    } catch (Exception e) {
      log.warn("Got invalid policy update message from server: {}", e.toString());
    }
  }

  void onDataNotification(String topic, JsonNode data) {
    if (data == null || data.isNull()) {
      log.warn("got data update message without data, skipping data update!");
      return;
    }
    try {
      dataUpdater.updatePolicyData(withId(Rpc.MAPPER.treeToValue(data, Data.DataUpdate.class)));
    } catch (Exception e) {
      log.warn("Got invalid data update message from server: {}", e.toString());
    }
  }

  /** R56: the base data configuration, from the server or from the configured URL. */
  public void fetchBaseDataConfiguration(String reason) {
    try {
      Data.DataSourceConfig sourcesConfig = readDataSourceConfig();
      dataUpdater.getBasePolicyData(sourcesConfig, reason);
    } catch (Exception e) {
      log.error("Failed to load data sources config", e);
    }
  }

  /**
   * R254: an update with no id of its own is given one.
   *
   * <p>The id is what ties a report back to the update that caused it, and a caller registering a
   * callback has nothing else to match on. An update published without one would otherwise
   * produce reports whose {@code update_id} is empty for every update in the fleet.
   */
  public static Data.DataUpdate withId(Data.DataUpdate update) {
    if (update == null || (update.id() != null && !update.id().isEmpty())) {
      return update;
    }
    String id = java.util.UUID.randomUUID().toString().replace("-", "");
    log.info("Triggering data update with id: {}", id);
    return new Data.DataUpdate(id, update.entries(), update.reason(), update.callback());
  }

  /** R262: the client's own connection-retry settings, as the fetching engine reads them. */
  public static io.akka.opal.common.fetcher.Retries.Config retryPolicy(Object options) {
    if (!(options instanceof io.akka.opal.common.config.Options.ConnRetryOptions retry)) {
      return io.akka.opal.common.fetcher.Retries.Config.defaults();
    }
    return new io.akka.opal.common.fetcher.Retries.Config(
        retry.attempts(), retry.wait_time(), retry.max_wait());
  }

  Data.DataSourceConfig readDataSourceConfig() throws Exception {
    // R253: a client belonging to a scope reads that scope's own data configuration. Reading the
    // server-wide one instead gives it another tenant's sources, which it would then fetch.
    String scopeId = config.getString("SCOPE_ID");
    String url =
        scopeId == null || scopeId.isEmpty() || "default".equals(scopeId)
            ? config.getString("DEFAULT_DATA_SOURCES_CONFIG_URL")
            : config.getString("SERVER_URL") + "/scopes/" + scopeId + "/data";
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET();
    String token = config.getString("CLIENT_TOKEN");
    if (token != null) {
      request.header("Authorization", "Bearer " + token);
    }
    HttpResponse<String> response =
        io.akka.opal.common.util.Http.forClient()
            .send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "could not read the data sources config: " + response.statusCode());
    }
    return Rpc.MAPPER.readValue(response.body(), Data.DataSourceConfig.class);
  }

  // -- offline mode --------------------------------------------------------

  /** R100: the backup is loaded at start-up, written on an interval and once at shutdown. */
  public synchronized void loadStoreFromBackup() {
    try {
      if (Files.isRegularFile(storeBackupPath)) {
        log.info("importing policy store from backup file...");
        store.fullImport(Files.readString(storeBackupPath, StandardCharsets.UTF_8));
        log.debug("import completed");
        backupLoaded = true;
      } else {
        log.warn("policy store backup file wasn't found");
      }
    } catch (Exception e) {
      log.error("failed to load backup data to policy store", e);
    }
  }

  /** Written to a temporary file beside the target and moved into place, so a reader never
   * sees half a backup. */
  public synchronized void backupStore() {
    try {
      Path parent = storeBackupPath.toAbsolutePath().getParent();
      Files.createDirectories(parent);
      Path temporary = Files.createTempFile(parent, "opal-backup", ".json.tmp");
      log.debug("exporting policy store to backup file...");
      Files.writeString(temporary, store.fullExport(), StandardCharsets.UTF_8);
      log.debug("export completed");
      Files.move(temporary, storeBackupPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      log.error("failed to backup policy store", e);
    }
  }

  private void startPeriodicBackup() {
    backupScheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-store-backup");
              thread.setDaemon(true);
              return thread;
            });
    backupTask =
        backupScheduler.scheduleWithFixedDelay(
            this::backupStore, storeBackupInterval, storeBackupInterval, TimeUnit.SECONDS);
  }

  public boolean backupLoaded() {
    return backupLoaded;
  }

  /** Whether this client is running the policy engine itself, rather than talking to one. */
  public boolean engineRunning() {
    return engineRunner != null;
  }

  // -- connectivity --------------------------------------------------------

  /** R99: stop the updaters, and — under offline mode — write a backup on the way out. */
  public synchronized boolean disableServerConnectivity() {
    if (connectivityDisabled) {
      return false;
    }
    log.warn("Disabling OPAL server connectivity at runtime");
    connectivityDisabled = true;
    stopUpdaters();
    if (offlineModeEnabled) {
      backupStore();
    }
    return true;
  }

  /**
   * R249: turning connectivity on at runtime, and saying so honestly when it did not work.
   *
   * <p>The flag is what {@code GET /healthy} reports as {@code online}. Leaving it on after the
   * updaters failed to start would have the client report itself connected while nothing is
   * listening, which is the one answer worse than being offline.
   */
  public synchronized boolean enableServerConnectivity() {
    if (!connectivityDisabled) {
      return false;
    }
    log.warn("Enabling OPAL server connectivity at runtime");
    connectivityDisabled = false;
    try {
      startUpdaters();
    } catch (RuntimeException e) {
      log.error("Runtime enable failed, reverting to disconnected state", e);
      connectivityDisabled = true;
      return false;
    }
    return true;
  }

  public synchronized void stopUpdaters() {
    if (policyChannel != null) {
      policyChannel.close();
      policyChannel = null;
    }
    if (dataChannel != null) {
      dataChannel.close();
      dataChannel = null;
    }
    dataUpdater.stopPollingUpdateTasks();
  }

  // -- health --------------------------------------------------------------

  /** R91: offline mode reports ready-but-offline rather than unhealthy. */
  public boolean healthy() {
    return store.isHealthy();
  }

  /**
   * R248: a client restored from a backup is ready, whether or not the server has been heard from.
   *
   * <p>Readiness is about whether this client can answer a policy question, and one that loaded a
   * backup can. Reading it from the store alone leaves an offline client answering 503 forever,
   * because the store's own readiness is set by a transaction from the server that will not come.
   */
  public boolean ready() {
    return backupLoaded || store.isReady();
  }

  public boolean online() {
    return !connectivityDisabled;
  }

  @Override
  public void close() {
    stopUpdaters();
    if (backupTask != null) {
      backupTask.cancel(true);
    }
    if (backupScheduler != null) {
      backupScheduler.shutdownNow();
    }
    if (offlineModeEnabled) {
      backupStore();
    }
    if (livenessProbe != null) {
      livenessProbe.close();
    }
    if (engineRunner != null) {
      engineRunner.close();
    }
  }

  /** What {@code GET /policy-store/config} reports. */
  public Map<String, Object> policyStoreDetails() {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("url", config.getString("POLICY_STORE_URL"));
    details.put("token", tokenIfNotExcluded());
    details.put("auth_type", ((io.akka.opal.common.config.Enums.PolicyStoreAuth)
        config.get("POLICY_STORE_AUTH_TYPE")).wire());
    details.put("type", ((PolicyStoreTypes) config.get("POLICY_STORE_TYPE")).wire());
    // R256: the three fields an OAuth-authenticated store needs, with the secret held back on the
    // same terms as the bearer token — a caller reading this is being told where the store is and
    // how to talk to it, and the secret is the one part of that a log should never carry.
    details.put("oauth_client_id", config.getString("POLICY_STORE_AUTH_OAUTH_CLIENT_ID"));
    details.put(
        "oauth_client_secret",
        Boolean.TRUE.equals(config.get("EXCLUDE_POLICY_STORE_SECRETS"))
            ? null
            : config.getString("POLICY_STORE_AUTH_OAUTH_CLIENT_SECRET"));
    details.put("oauth_server", config.getString("POLICY_STORE_AUTH_OAUTH_SERVER"));
    details.values().removeIf(java.util.Objects::isNull);
    return details;
  }

  private String tokenIfNotExcluded() {
    if (Boolean.TRUE.equals(config.get("EXCLUDE_POLICY_STORE_SECRETS"))) {
      return null;
    }
    return config.getString("POLICY_STORE_AUTH_TOKEN");
  }
}
