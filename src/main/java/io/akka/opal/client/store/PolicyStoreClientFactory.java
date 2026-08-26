package io.akka.opal.client.store;

import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Enums.PolicyStoreTypes;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds the configured policy store, and hands back the same one for the same type and URL.
 *
 * <p>The cache is not an optimisation. The store carries the transaction log that decides what
 * {@code /healthy} answers, so a second instance for the same engine would answer a different
 * question about the same thing.
 */
public final class PolicyStoreClientFactory {

  /** Raised when the configured type names no store. */
  public static final class InvalidPolicyStoreType extends RuntimeException {
    public InvalidPolicyStoreType(String message) {
      super(message);
    }
  }

  private static final Map<String, PolicyStoreClient> CACHE = new ConcurrentHashMap<>();

  private PolicyStoreClientFactory() {}

  public static String cacheKey(PolicyStoreTypes storeType, String url) {
    return storeType.wire() + "|" + url;
  }

  public static PolicyStoreClient get(ClientConfig config) {
    PolicyStoreTypes storeType = config.get("POLICY_STORE_TYPE");
    String url = config.getString("POLICY_STORE_URL");
    PolicyStoreClient cached = CACHE.get(cacheKey(storeType, url));
    return cached != null ? cached : create(config, true);
  }

  public static PolicyStoreClient create(ClientConfig config, boolean saveToCache) {
    PolicyStoreTypes storeType = config.get("POLICY_STORE_TYPE");
    String url = config.getString("POLICY_STORE_URL");
    String token = config.getString("POLICY_STORE_AUTH_TOKEN");
    PolicyStoreAuth authType = config.get("POLICY_STORE_AUTH_TYPE");
    boolean offlineMode = Boolean.TRUE.equals(config.get("OFFLINE_MODE_ENABLED"));
    List<String> pathsToIgnore = config.get("POLICY_STORE_POLICY_PATHS_TO_IGNORE");
    ConnRetryOptions retry = config.get("POLICY_STORE_CONN_RETRY");

    PolicyStoreClient store =
        switch (storeType) {
          case OPA ->
              new OpaClient(
                  url,
                  token,
                  authType,
                  config.getString("POLICY_STORE_AUTH_OAUTH_CLIENT_ID"),
                  config.getString("POLICY_STORE_AUTH_OAUTH_CLIENT_SECRET"),
                  config.getString("POLICY_STORE_AUTH_OAUTH_SERVER"),
                  Boolean.TRUE.equals(config.get("DATA_UPDATER_ENABLED")),
                  Boolean.TRUE.equals(config.get("POLICY_UPDATER_ENABLED")),
                  offlineMode,
                  config.getString("POLICY_STORE_TLS_CLIENT_CERT"),
                  config.getString("POLICY_STORE_TLS_CLIENT_KEY"),
                  config.getString("POLICY_STORE_TLS_CA"),
                  pathsToIgnore,
                  ClientConfig.OPA_HEALTH_CHECK_POLICY_PATH,
                  retry);
          case CEDAR -> new CedarClient(url, token, authType, pathsToIgnore, retry);
          case MOCK -> new MockPolicyStoreClient();
        };
    if (saveToCache) {
      CACHE.put(cacheKey(storeType, url), store);
    }
    return store;
  }

  /** Only the tests and a restart need this; a running client keeps one store for its life. */
  public static void clearCache() {
    CACHE.clear();
  }
}
