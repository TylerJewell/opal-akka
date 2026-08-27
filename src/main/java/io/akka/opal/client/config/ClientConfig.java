package io.akka.opal.client.config;

import io.akka.opal.common.confi.Confi;
import io.akka.opal.common.confi.ConfiEntry;
import io.akka.opal.common.config.Enums.EngineLogFormat;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Enums.PolicyStoreTypes;
import io.akka.opal.common.config.Options.CedarServerOptions;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.config.Options.OpaServerOptions;
import io.akka.opal.common.config.Options.WaitStrategy;
import io.akka.opal.common.schemas.Data.HttpFetcherConfig;
import io.akka.opal.common.schemas.Data.UpdateCallback;
import java.util.List;
import java.util.Map;

/**
 * The 55 entries only the client has. SPEC-002 R6.
 *
 * <p>Generated from the census taken off the running source by
 * {@code opal-port/probes/complete/gen_config.py}, so that the set is the source's own
 * rather than a transcription of it. Declaration order is the source's declaration
 * order, which is also evaluation order.
 */
public final class ClientConfig extends Confi {

  @Override
  protected String configName() {
    return "OpalClientConfig";
  }


  /** Not configurable: the module id the transaction-log policy is written under. */
  public static final String OPA_HEALTH_CHECK_POLICY_PATH = "engine/healthcheck/opal.rego";


  public ClientConfig(Map<String, String> environment) {
    super("OPAL_", environment);
    declare();
    resolveDelayed();
  }

  public ClientConfig() {
    this(io.akka.opal.common.confi.ConfigFiles.overlay(System.getenv()));
  }

  private void declare() {
    enumeration("POLICY_STORE_TYPE", PolicyStoreTypes.class, PolicyStoreTypes.OPA, "The type of policy store to use (e.g., OPA, Cedar, etc.)");
    str("POLICY_STORE_URL", "http://localhost:8181", "The URL of the policy store (e.g., OPA agent).");
    enumeration("POLICY_STORE_AUTH_TYPE", PolicyStoreAuth.class, PolicyStoreAuth.NONE, "The authentication type to use for the policy store (e.g., NONE, TOKEN, etc.)");
    str("POLICY_STORE_AUTH_TOKEN", null, "The authentication (bearer) token OPAL client will use to authenticate against the policy store (i.e: OPA agent).");
    str("POLICY_STORE_AUTH_OAUTH_SERVER", null, "The authentication server OPAL client will use to authenticate against for retrieving the access_token.");
    str("POLICY_STORE_AUTH_OAUTH_CLIENT_ID", null, "The client_id OPAL will use to authenticate against the OAuth server.");
    str("POLICY_STORE_AUTH_OAUTH_CLIENT_SECRET", null, "The client secret OPAL will use to authenticate against the OAuth server.");
    model("POLICY_STORE_CONN_RETRY", ConnRetryOptions.class,
            ConnRetryOptions.defaults(), "Retry options when connecting to the policy store (i.e. the agent that handles the policy, e.g. OPA)");
    bool("POLICY_STORE_LIVENESS_PROBE_ENABLED", true, "If True, OPAL client periodically probes the policy store's health endpoint and factors the result into /healthy. This makes /healthy reflect live policy-store responsiveness, not just the success of the last server -> policy-store transaction.");
    integer("POLICY_STORE_LIVENESS_PROBE_INTERVAL_SECONDS", 10, "Interval (seconds) between background liveness probes against the policy store.");
    integer("POLICY_STORE_LIVENESS_PROBE_TIMEOUT_SECONDS", 2, "Per-request HTTP timeout (seconds) for liveness probes against the policy store.");
    model("POLICY_UPDATER_CONN_RETRY", ConnRetryOptions.class,
            new ConnRetryOptions(WaitStrategy.random_exponential, 1.0, 5, 10.0), "Retry options when connecting to the policy source (e.g. the policy bundle server)");
    model("DATA_STORE_CONN_RETRY", ConnRetryOptions.class, null, "DEPRECATED - The old confusing name for DATA_UPDATER_CONN_RETRY, kept for backwards compatibility (for now)");
    model("DATA_UPDATER_CONN_RETRY", ConnRetryOptions.class,
            new ConnRetryOptions(WaitStrategy.random_exponential, 1.0, 5, 10.0), "Retry options when connecting to the base data source (e.g. an external API server which returns data snapshot)");
    list("POLICY_STORE_POLICY_PATHS_TO_IGNORE", List.of(), "When loading policies manually or otherwise externally into the policy store, use this list of glob patterns to have OPAL ignore and not delete or override them, end paths (without any wildcards in the middle) with '/**' to indicate you want all nested under the path to be ignored");
    bool("POLICY_UPDATER_ENABLED", true, "If set to False, opal client will not listen to dynamic policy updates.Policy update fetching will be completely disabled.");
    str("POLICY_STORE_TLS_CLIENT_CERT", null, "Path to the client certificate used for TLS authentication with the policy store");
    str("POLICY_STORE_TLS_CLIENT_KEY", null, "Path to the client key used for TLS authentication with the policy store");
    str("POLICY_STORE_TLS_CA", null, "Path to the file containing the CA certificate(s) used for TLS authentication with the policy store");
    bool("EXCLUDE_POLICY_STORE_SECRETS", false, "If set, policy store secrets will be excluded from the /policy-store/config route");
    bool("INLINE_OPA_ENABLED", true, "Whether or not OPAL should run OPA by itself in the same container");
    str("INLINE_OPA_EXEC_PATH", null, "Path to the OPA executable. Defaults to searching for 'opa' binary in PATH if not specified.");
    model("INLINE_OPA_CONFIG", OpaServerOptions.class, OpaServerOptions.defaults(), "CLI options used when running `opa run --server` inline");
    bool("OPA_V0_COMPAT", true, "Set to true to enable OPA v0 compatibility mode (--v0-compatible flag). This merges with INLINE_OPA_CONFIG.v0_compatible - if either is enabled, the flag will be added to OPA");
    enumeration("INLINE_OPA_LOG_FORMAT", EngineLogFormat.class, EngineLogFormat.NONE, "The log format to use for inline OPA logs");
    bool("INLINE_CEDAR_ENABLED", true, "Whether or not OPAL should run the Cedar agent by itself in the same container");
    str("INLINE_CEDAR_EXEC_PATH", null, "Path to the Cedar Agent executable. Defaults to searching for 'cedar-agent' binary in PATH if not specified.");
    model("INLINE_CEDAR_CONFIG", CedarServerOptions.class,
            CedarServerOptions.defaults(), "CLI options used when running the Cedar agent inline");
    enumeration("INLINE_CEDAR_LOG_FORMAT", EngineLogFormat.class, EngineLogFormat.NONE, "The log format to use for inline Cedar logs");
    integer("KEEP_ALIVE_INTERVAL", 0, "The interval (in seconds) for sending keep-alive messages");
    str("SERVER_URL", "SERVER_URL", "http://localhost:7002", "The URL of the OPAL server",
            List.of("-s"));
    strDelayed("SERVER_WS_URL",
            c -> c.getString("SERVER_URL").replace("https", "wss").replace("http", "ws"),
            "The WebSocket URL of the OPAL server");
    strDelayed("SERVER_PUBSUB_URL", c -> c.getString("SERVER_WS_URL") + "/ws", "The Pub/Sub URL of the OPAL server");
    str("CLIENT_TOKEN", "CLIENT_TOKEN", "THIS_IS_A_DEV_SECRET", "The authentication token for the OPAL server",
            List.of("-t"));
    integer("CLIENT_API_SERVER_WORKER_COUNT", 1, "(if run via CLI) Worker count for the opal-client's internal server");
    str("CLIENT_API_SERVER_HOST", "127.0.0.1", "(if run via CLI)  Address for the opal-client's internal server to bind");
    integer("CLIENT_API_SERVER_PORT", 7000, "(if run via CLI)  Port for the opal-client's internal server to bind");
    bool("WAIT_ON_SERVER_LOAD", false, "If set, client would wait for 200 from server's loadlimit endpoint before starting background tasks");
    list("POLICY_SUBSCRIPTION_DIRS", List.of("."), "Directories in the policy repo to subscribe to for policy code (rego) modules", ":");
    bool("DATA_UPDATER_ENABLED", true, "If set to False, opal client will not listen to dynamic data updates. Dynamic data fetching will be completely disabled.");
    list("DATA_TOPICS", List.of("policy_data"), "Data topics to subscribe to");
    strDelayed("DEFAULT_DATA_SOURCES_CONFIG_URL",
            c -> c.getString("SERVER_URL") + "/data/config", "Default URL to fetch data configuration from");
    str("DEFAULT_DATA_URL", "http://localhost:8000/policy-config", "Default URL to fetch data from");
    bool("SHOULD_REPORT_ON_DATA_UPDATES", false, "Should the client report on updates to callbacks defined in DEFAULT_UPDATE_CALLBACKS or within the given updates");
    model("DEFAULT_UPDATE_CALLBACK_CONFIG", HttpFetcherConfig.class,
            HttpFetcherConfig.defaultCallbackConfig(), "Configuration for the default update callback");
    modelDelayed("DEFAULT_UPDATE_CALLBACKS", UpdateCallback.class,
            c -> new UpdateCallback(
                List.of(c.getString("SERVER_URL") + "/data/callback_report")), "Where/How the client should report on the completion of data updates");
    bool("OPA_HEALTH_CHECK_POLICY_ENABLED", false, "Should we load a special healthcheck policy into OPA that checks that opa was synced correctly and is ready to answer to authorization queries");
    str("OPA_HEALTH_CHECK_TRANSACTION_LOG_PATH", "system/opal/transactions", "Path to OPA document that stores the OPA write transactions");
    str("OPAL_CLIENT_STAT_ID", null, "Unique client statistics identifier");
    str("SCOPE_ID", "default", "OPAL Scope ID");
    str("STORE_BACKUP_PATH", "/opal/backup/opa.json", "Path to backup policy store's data to");
    integer("STORE_BACKUP_INTERVAL", 60, "Interval in seconds to backup policy store's data");
    bool("OFFLINE_MODE_ENABLED", false, "If set, opal client will try to load policy store from backup file and operate even if server is unreachable. Ignored if INLINE_OPA_ENABLED=False");
    bool("DEFAULT_OPAL_SERVER_CONNECTIVITY_DISABLED", false, "If set together with OFFLINE_MODE_ENABLED, the client will not connect to the OPAL server when a valid backup is loaded. Can be toggled at runtime via the /opal-server/connectivity endpoints.");
    bool("SPLIT_ROOT_DATA", false, "Split writing data updates to root path");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<PolicyStoreTypes> POLICY_STORE_TYPE() {
    return (ConfiEntry<PolicyStoreTypes>) entries().get("POLICY_STORE_TYPE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_URL() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<PolicyStoreAuth> POLICY_STORE_AUTH_TYPE() {
    return (ConfiEntry<PolicyStoreAuth>) entries().get("POLICY_STORE_AUTH_TYPE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_AUTH_TOKEN() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_AUTH_TOKEN");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_AUTH_OAUTH_SERVER() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_AUTH_OAUTH_SERVER");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_AUTH_OAUTH_CLIENT_ID() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_AUTH_OAUTH_CLIENT_ID");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_AUTH_OAUTH_CLIENT_SECRET() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_AUTH_OAUTH_CLIENT_SECRET");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<ConnRetryOptions> POLICY_STORE_CONN_RETRY() {
    return (ConfiEntry<ConnRetryOptions>) entries().get("POLICY_STORE_CONN_RETRY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> POLICY_STORE_LIVENESS_PROBE_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("POLICY_STORE_LIVENESS_PROBE_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> POLICY_STORE_LIVENESS_PROBE_INTERVAL_SECONDS() {
    return (ConfiEntry<Integer>) entries().get("POLICY_STORE_LIVENESS_PROBE_INTERVAL_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> POLICY_STORE_LIVENESS_PROBE_TIMEOUT_SECONDS() {
    return (ConfiEntry<Integer>) entries().get("POLICY_STORE_LIVENESS_PROBE_TIMEOUT_SECONDS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<ConnRetryOptions> POLICY_UPDATER_CONN_RETRY() {
    return (ConfiEntry<ConnRetryOptions>) entries().get("POLICY_UPDATER_CONN_RETRY");
  }

  /**
   * The two adjustments loading a client's configuration makes — SPEC-002 R152 and R153.
   *
   * <p>Neither is a value a deployment sets; both are what the configuration does to itself
   * once it has been read. The first silences the engine's own log module when the client is
   * not piping its logs, so a deployment that turned piping off does not get the module's
   * output by another route. The second carries a deprecated entry into the one that replaced
   * it, so a deployment still setting the old name is not silently running on the default.
   *
   * <p>Applied against the common configuration rather than this one, because the list the
   * first touches belongs there — which is also why it is a method a caller invokes rather
   * than something the constructor does: this object does not own the other one.
   */
  public void onLoad(io.akka.opal.common.config.CommonConfig common) {
    if (get("INLINE_OPA_LOG_FORMAT") == EngineLogFormat.NONE) {
      java.util.List<String> excluded =
          new java.util.ArrayList<>(common.get("LOG_MODULE_EXCLUDE_LIST"));
      if (!excluded.contains("opal_client.opa.logger")) {
        excluded.add("opal_client.opa.logger");
      }
      common.LOG_MODULE_EXCLUDE_LIST().set(excluded);
    }
    ConnRetryOptions deprecated = get("DATA_STORE_CONN_RETRY");
    if (deprecated != null) {
      DATA_UPDATER_CONN_RETRY().set(deprecated);
    }
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<ConnRetryOptions> DATA_STORE_CONN_RETRY() {
    return (ConfiEntry<ConnRetryOptions>) entries().get("DATA_STORE_CONN_RETRY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<ConnRetryOptions> DATA_UPDATER_CONN_RETRY() {
    return (ConfiEntry<ConnRetryOptions>) entries().get("DATA_UPDATER_CONN_RETRY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> POLICY_STORE_POLICY_PATHS_TO_IGNORE() {
    return (ConfiEntry<List<String>>) entries().get("POLICY_STORE_POLICY_PATHS_TO_IGNORE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> POLICY_UPDATER_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("POLICY_UPDATER_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_TLS_CLIENT_CERT() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_TLS_CLIENT_CERT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_TLS_CLIENT_KEY() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_TLS_CLIENT_KEY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> POLICY_STORE_TLS_CA() {
    return (ConfiEntry<String>) entries().get("POLICY_STORE_TLS_CA");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> EXCLUDE_POLICY_STORE_SECRETS() {
    return (ConfiEntry<Boolean>) entries().get("EXCLUDE_POLICY_STORE_SECRETS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> INLINE_OPA_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("INLINE_OPA_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> INLINE_OPA_EXEC_PATH() {
    return (ConfiEntry<String>) entries().get("INLINE_OPA_EXEC_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<OpaServerOptions> INLINE_OPA_CONFIG() {
    return (ConfiEntry<OpaServerOptions>) entries().get("INLINE_OPA_CONFIG");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> OPA_V0_COMPAT() {
    return (ConfiEntry<Boolean>) entries().get("OPA_V0_COMPAT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<EngineLogFormat> INLINE_OPA_LOG_FORMAT() {
    return (ConfiEntry<EngineLogFormat>) entries().get("INLINE_OPA_LOG_FORMAT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> INLINE_CEDAR_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("INLINE_CEDAR_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> INLINE_CEDAR_EXEC_PATH() {
    return (ConfiEntry<String>) entries().get("INLINE_CEDAR_EXEC_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<CedarServerOptions> INLINE_CEDAR_CONFIG() {
    return (ConfiEntry<CedarServerOptions>) entries().get("INLINE_CEDAR_CONFIG");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<EngineLogFormat> INLINE_CEDAR_LOG_FORMAT() {
    return (ConfiEntry<EngineLogFormat>) entries().get("INLINE_CEDAR_LOG_FORMAT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> KEEP_ALIVE_INTERVAL() {
    return (ConfiEntry<Integer>) entries().get("KEEP_ALIVE_INTERVAL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SERVER_URL() {
    return (ConfiEntry<String>) entries().get("SERVER_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SERVER_WS_URL() {
    return (ConfiEntry<String>) entries().get("SERVER_WS_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SERVER_PUBSUB_URL() {
    return (ConfiEntry<String>) entries().get("SERVER_PUBSUB_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> CLIENT_TOKEN() {
    return (ConfiEntry<String>) entries().get("CLIENT_TOKEN");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> CLIENT_API_SERVER_WORKER_COUNT() {
    return (ConfiEntry<Integer>) entries().get("CLIENT_API_SERVER_WORKER_COUNT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> CLIENT_API_SERVER_HOST() {
    return (ConfiEntry<String>) entries().get("CLIENT_API_SERVER_HOST");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> CLIENT_API_SERVER_PORT() {
    return (ConfiEntry<Integer>) entries().get("CLIENT_API_SERVER_PORT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> WAIT_ON_SERVER_LOAD() {
    return (ConfiEntry<Boolean>) entries().get("WAIT_ON_SERVER_LOAD");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> POLICY_SUBSCRIPTION_DIRS() {
    return (ConfiEntry<List<String>>) entries().get("POLICY_SUBSCRIPTION_DIRS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> DATA_UPDATER_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("DATA_UPDATER_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> DATA_TOPICS() {
    return (ConfiEntry<List<String>>) entries().get("DATA_TOPICS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> DEFAULT_DATA_SOURCES_CONFIG_URL() {
    return (ConfiEntry<String>) entries().get("DEFAULT_DATA_SOURCES_CONFIG_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> DEFAULT_DATA_URL() {
    return (ConfiEntry<String>) entries().get("DEFAULT_DATA_URL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> SHOULD_REPORT_ON_DATA_UPDATES() {
    return (ConfiEntry<Boolean>) entries().get("SHOULD_REPORT_ON_DATA_UPDATES");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<HttpFetcherConfig> DEFAULT_UPDATE_CALLBACK_CONFIG() {
    return (ConfiEntry<HttpFetcherConfig>) entries().get("DEFAULT_UPDATE_CALLBACK_CONFIG");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<UpdateCallback> DEFAULT_UPDATE_CALLBACKS() {
    return (ConfiEntry<UpdateCallback>) entries().get("DEFAULT_UPDATE_CALLBACKS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> OPA_HEALTH_CHECK_POLICY_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("OPA_HEALTH_CHECK_POLICY_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> OPA_HEALTH_CHECK_TRANSACTION_LOG_PATH() {
    return (ConfiEntry<String>) entries().get("OPA_HEALTH_CHECK_TRANSACTION_LOG_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> OPAL_CLIENT_STAT_ID() {
    return (ConfiEntry<String>) entries().get("OPAL_CLIENT_STAT_ID");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> SCOPE_ID() {
    return (ConfiEntry<String>) entries().get("SCOPE_ID");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STORE_BACKUP_PATH() {
    return (ConfiEntry<String>) entries().get("STORE_BACKUP_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> STORE_BACKUP_INTERVAL() {
    return (ConfiEntry<Integer>) entries().get("STORE_BACKUP_INTERVAL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> OFFLINE_MODE_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("OFFLINE_MODE_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> DEFAULT_OPAL_SERVER_CONNECTIVITY_DISABLED() {
    return (ConfiEntry<Boolean>) entries().get("DEFAULT_OPAL_SERVER_CONNECTIVITY_DISABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> SPLIT_ROOT_DATA() {
    return (ConfiEntry<Boolean>) entries().get("SPLIT_ROOT_DATA");
  }

}
