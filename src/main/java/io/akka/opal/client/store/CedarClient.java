package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.util.Http;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Cedar agent — SPEC-002 R86.
 *
 * <p>Cedar's data document is replaced whole rather than by path, so every data route here
 * refuses a path. Its readiness and health are tracked on this class rather than through the
 * transaction-log state, because Cedar has no equivalent of the generated Rego module and OPAL
 * therefore keeps a shorter record for it.
 */
public final class CedarClient implements PolicyStoreClient {

  private static final Logger log = LoggerFactory.getLogger(CedarClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String baseUrl;
  private final String apiUrl;
  private final HttpClient http = Http.plain();
  private final ConnRetryOptions retry;
  private final PolicyStoreAuth authType;
  private final String token;
  private final List<String> pathsToIgnore;

  private volatile String policyVersion;
  private volatile boolean hadSuccessfulPolicyTransaction;
  private volatile boolean hadSuccessfulDataTransaction;
  private volatile Store.StoreTransaction mostRecentPolicyTransaction;
  private volatile Store.StoreTransaction mostRecentDataTransaction;
  private volatile boolean engineReachable = true;

  public CedarClient(
      String cedarServerUrl,
      String cedarAuthToken,
      PolicyStoreAuth authType,
      List<String> pathsToIgnore,
      ConnRetryOptions retry) {
    this.baseUrl = cedarServerUrl;
    this.apiUrl = cedarServerUrl + "/v1";
    this.token = cedarAuthToken;
    this.authType = authType;
    this.pathsToIgnore = pathsToIgnore == null ? List.of() : pathsToIgnore;
    this.retry = retry == null ? ConnRetryOptions.defaults() : retry;

    if (authType == PolicyStoreAuth.TOKEN && cedarAuthToken == null) {
      log.error("POLICY_STORE_AUTH_TOKEN can not be empty");
      throw new IllegalStateException("required variables for token auth are not set");
    }
    if (authType == PolicyStoreAuth.OAUTH) {
      throw new IllegalArgumentException("Cedar doesn't support OAuth.");
    }
    log.info("Authentication mode for policy store: {}", authType.wire());
  }

  public String baseUrl() {
    return baseUrl;
  }

  public String apiUrl() {
    return apiUrl;
  }

  public HttpClient http() {
    return http;
  }

  public boolean engineReachable() {
    return engineReachable;
  }

  public void setEngineReachable(boolean value) {
    this.engineReachable = value;
  }

  private Map<String, String> authHeaders() {
    Map<String, String> headers = new LinkedHashMap<>();
    if (authType == PolicyStoreAuth.TOKEN && token != null) {
      headers.put("Authorization", "Bearer " + token);
    }
    return headers;
  }

  private HttpResponse<String> send(HttpRequest.Builder builder, List<Integer> accepted) {
    Exception last = null;
    for (int attempt = 1; attempt <= retry.attempts(); attempt++) {
      try {
        authHeaders().forEach(builder::header);
        HttpResponse<String> response =
            http.send(
                builder.timeout(Duration.ofSeconds(30)).build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (accepted != null && !accepted.contains(response.statusCode())) {
          throw new IllegalStateException(
              "Cedar Client: unexpected status code: "
                  + response.statusCode()
                  + ", error: "
                  + response.body());
        }
        return response;
      } catch (Exception e) {
        last = e;
        log.warn("Cedar Agent connection error: {}", e.toString());
        try {
          Thread.sleep(
              retry.waitMillis(attempt, java.util.concurrent.ThreadLocalRandom.current()));
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    throw new IllegalStateException("Cedar request failed", last);
  }

  private static String quotePlus(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override
  public void setPolicy(String policyId, String policyCode, String transactionId) {
    if (IgnorePaths.shouldIgnorePath(policyId, pathsToIgnore)) {
      log.info(
          "Ignoring setting policy - {}, set in POLICY_STORE_POLICY_PATHS_TO_IGNORE.", policyId);
      return;
    }
    ObjectNode body = MAPPER.createObjectNode();
    body.put("content", policyCode);
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + quotePlus(policyId)))
            .header("content-type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8)),
        List.of(200, 400));
  }

  @Override
  public String getPolicy(String policyId) {
    try {
      HttpResponse<String> response =
          send(
              HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + quotePlus(policyId))).GET(),
              null);
      JsonNode raw = MAPPER.readTree(response.body()).path("result").path("raw");
      return raw.isMissingNode() || raw.isNull() ? null : raw.asText();
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Map<String, String> getPolicies() {
    try {
      HttpResponse<String> response =
          send(HttpRequest.newBuilder(URI.create(apiUrl + "/policies")).GET(), null);
      Map<String, String> out = new LinkedHashMap<>();
      for (JsonNode policy : MAPPER.readTree(response.body())) {
        out.put(policy.path("id").asText(), policy.path("content").asText());
      }
      return out;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void deletePolicy(String policyId, String transactionId) {
    if (IgnorePaths.shouldIgnorePath(policyId, pathsToIgnore)) {
      log.info(
          "Ignoring deleting policy - {}, set in POLICY_STORE_POLICY_PATHS_TO_IGNORE.", policyId);
      return;
    }
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + quotePlus(policyId))).DELETE(),
        List.of(204, 404));
  }

  @Override
  public List<String> getPolicyModuleIds() {
    Map<String, String> policies = getPolicies();
    return policies == null ? List.of() : new ArrayList<>(policies.keySet());
  }

  @Override
  public void setPolicies(Policy.PolicyBundle bundle, String transactionId) {
    for (Policy.RegoModule policy : bundle.policy_modules()) {
      setPolicy(policy.path(), policy.rego(), null);
    }
    List<String> deletedModules = new ArrayList<>();
    if (bundle.old_hash() == null) {
      Set<String> inStore = new LinkedHashSet<>(getPolicyModuleIds());
      for (Policy.RegoModule policy : bundle.policy_modules()) {
        inStore.remove(policy.path());
      }
      deletedModules.addAll(inStore);
    } else if (bundle.deleted_files() != null) {
      deletedModules.addAll(bundle.deleted_files().policy_modules());
    }
    for (String moduleId : deletedModules) {
      deletePolicy(moduleId, null);
    }
    policyVersion = bundle.hash();
  }

  /** R86: Cedar reports its version from the store rather than holding one locally. */
  @Override
  public String getPolicyVersion() {
    return policyVersion;
  }

  @Override
  public void setPolicyData(JsonNode policyData, String path, String transactionId) {
    if (path != null && !path.isEmpty()) {
      throw new IllegalArgumentException("Cedar can only change the entire data structure at once.");
    }
    if (policyData == null || !policyData.isArray()) {
      log.warn(
          "OPAL client was instructed to put something that is not a list on Cedar. This will"
              + " probably not work.");
    }
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/data"))
            .header("content-type", "application/json")
            .PUT(
                HttpRequest.BodyPublishers.ofString(
                    policyData == null ? "null" : policyData.toString(), StandardCharsets.UTF_8)),
        List.of(200, 204, 304));
  }

  @Override
  public void patchPolicyData(List<Store.JSONPatchAction> actions, String path,
      String transactionId) {
    throw new IllegalArgumentException("Cedar can only change the entire data structure at once.");
  }

  @Override
  public void deletePolicyData(String path, String transactionId) {
    if (path != null && !path.isEmpty()) {
      throw new IllegalArgumentException("Cedar can only change the entire data structure at once.");
    }
    send(HttpRequest.newBuilder(URI.create(apiUrl + "/data")).DELETE(), List.of(204, 404));
  }

  @Override
  public JsonNode getData(String path) {
    if (path != null && !path.isEmpty()) {
      throw new IllegalArgumentException("Cedar can only change the entire data structure at once.");
    }
    try {
      HttpResponse<String> response =
          send(HttpRequest.newBuilder(URI.create(apiUrl + "/data")).GET(), null);
      return MAPPER.readTree(response.body());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Proxied getDataWithInput(String path, JsonNode input) {
    throw new UnsupportedOperationException("Cedar has no document-with-input query");
  }

  @Override
  public void initHealthcheckPolicy(String policyId, String policyCode) {
    throw new UnsupportedOperationException("Cedar has no transaction-log policy");
  }

  @Override
  public void logTransaction(Store.StoreTransaction transaction) {
    if (transaction.transaction_type() == Store.TransactionType.policy) {
      mostRecentPolicyTransaction = transaction;
      if (transaction.success()) {
        hadSuccessfulPolicyTransaction = true;
      }
    } else if (transaction.transaction_type() == Store.TransactionType.data) {
      mostRecentDataTransaction = transaction;
      if (transaction.success()) {
        hadSuccessfulDataTransaction = true;
      }
    }
  }

  @Override
  public boolean isReady() {
    return hadSuccessfulPolicyTransaction && hadSuccessfulDataTransaction;
  }

  @Override
  public boolean isHealthy() {
    boolean transactionsHealthy =
        mostRecentPolicyTransaction != null
            && mostRecentPolicyTransaction.success()
            && mostRecentDataTransaction != null
            && mostRecentDataTransaction.success();
    return transactionsHealthy && engineReachable;
  }

  @Override
  public String fullExport() {
    ObjectNode out = MAPPER.createObjectNode();
    out.set("policies", MAPPER.valueToTree(getPolicies()));
    out.set("data", getData(""));
    return out.toString();
  }

  @Override
  public void fullImport(String contents) {
    try {
      JsonNode imported = MAPPER.readTree(contents);
      JsonNode policies = imported.path("policies");
      policies
          .fieldNames()
          .forEachRemaining(id -> setPolicy(id, policies.get(id).asText(), null));
      setPolicyData(imported.path("data"), "", null);
    } catch (Exception e) {
      throw new IllegalStateException("could not import policy store backup", e);
    }
  }
}
