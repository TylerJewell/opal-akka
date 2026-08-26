package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.rego.Rego;
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
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OPA, through its REST API — SPEC-002 R76 to R85 and R90.
 *
 * <p>The accepted status codes per route are the source's: a 400 from a policy write is a bad
 * rego module and retrying it changes nothing, a 404 from a delete means the thing was already
 * gone, and a 304 from a data write means OPA already held that value. Anything else is an error
 * and is retried.
 */
public final class OpaClient implements PolicyStoreClient {

  private static final Logger log = LoggerFactory.getLogger(OpaClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String baseUrl;
  private final String apiUrl;
  private final HttpClient http;
  private final ConnRetryOptions retry;
  private final PolicyStoreAuth authType;
  private final String token;
  private final String oauthServer;
  private final String oauthClientId;
  private final String oauthClientSecret;
  private final List<String> pathsToIgnore;
  private final String healthCheckPolicyPath;
  private final TransactionLogState state;
  private final ReentrantLock lock = new ReentrantLock();
  private final OpaStaticDataCache dataCache;

  private volatile String policyVersion;
  private volatile String cachedOauthToken;
  private volatile long cachedOauthExpiry;
  private volatile TransactionLogPolicyWriter transactionWriter;

  public OpaClient(
      String opaServerUrl,
      String authToken,
      PolicyStoreAuth authType,
      String oauthClientId,
      String oauthClientSecret,
      String oauthServer,
      boolean dataUpdaterEnabled,
      boolean policyUpdaterEnabled,
      boolean cachePolicyData,
      String tlsClientCert,
      String tlsClientKey,
      String tlsCa,
      List<String> pathsToIgnore,
      String healthCheckPolicyPath,
      ConnRetryOptions retry) {
    this.baseUrl = opaServerUrl;
    this.apiUrl = opaServerUrl + "/v1";
    this.authType = authType;
    this.token = authToken;
    this.oauthClientId = oauthClientId;
    this.oauthClientSecret = oauthClientSecret;
    this.oauthServer = oauthServer;
    this.pathsToIgnore = pathsToIgnore == null ? List.of() : pathsToIgnore;
    this.healthCheckPolicyPath = healthCheckPolicyPath;
    this.retry = retry == null ? ConnRetryOptions.defaults() : retry;
    this.state = new TransactionLogState(dataUpdaterEnabled, policyUpdaterEnabled);
    this.dataCache = cachePolicyData ? new OpaStaticDataCache() : null;

    // R85: a mode missing any of its settings fails start-up, with one line per missing setting,
    // because an operator fixing three at once should not have to discover them one restart apart.
    if (authType == PolicyStoreAuth.TOKEN && authToken == null) {
      log.error("POLICY_STORE_AUTH_TOKEN can not be empty");
      throw new IllegalStateException("required variables for token auth are not set");
    }
    if (authType == PolicyStoreAuth.OAUTH) {
      boolean error = false;
      if (oauthClientId == null) {
        log.error("POLICY_STORE_AUTH_OAUTH_CLIENT_ID can not be empty");
        error = true;
      }
      if (oauthClientSecret == null) {
        log.error("POLICY_STORE_AUTH_OAUTH_CLIENT_SECRET can not be empty");
        error = true;
      }
      if (oauthServer == null) {
        log.error("POLICY_STORE_AUTH_OAUTH_SERVER can not be empty");
        error = true;
      }
      if (error) {
        throw new IllegalStateException("required variables for oauth are not set");
      }
    }
    if (authType == PolicyStoreAuth.TLS) {
      boolean error = false;
      if (tlsClientCert == null) {
        log.error("POLICY_STORE_TLS_CLIENT_CERT can not be empty");
        error = true;
      }
      if (tlsClientKey == null) {
        log.error("POLICY_STORE_TLS_CLIENT_KEY can not be empty");
        error = true;
      }
      if (tlsCa == null) {
        log.error("POLICY_STORE_TLS_CA can not be empty");
        error = true;
      }
      if (error) {
        throw new IllegalStateException("required variables for tls are not set");
      }
    }
    log.info("Authentication mode for policy store: {}", authType.wire());
    this.http = Http.withTls(tlsCa, tlsClientCert, tlsClientKey);
  }

  public TransactionLogState state() {
    return state;
  }

  public String baseUrl() {
    return baseUrl;
  }

  public HttpClient http() {
    return http;
  }

  // -- authentication ------------------------------------------------------

  private Map<String, String> authHeaders() {
    Map<String, String> headers = new LinkedHashMap<>();
    if (authType == PolicyStoreAuth.TOKEN && token != null) {
      headers.put("Authorization", "Bearer " + token);
    } else if (authType == PolicyStoreAuth.OAUTH) {
      if (cachedOauthToken == null || System.currentTimeMillis() / 1000 > cachedOauthExpiry) {
        refreshOauthToken();
      }
      headers.put("Authorization", "Bearer " + cachedOauthToken);
    }
    return headers;
  }

  /** R85: the token is cached until ten seconds before it expires. */
  private void refreshOauthToken() {
    log.info("Retrieving a new OAuth access_token.");
    String body = "grant_type=" + URLEncoder.encode("client_credentials", StandardCharsets.UTF_8);
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(oauthServer))
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded;charset=UTF-8")
            .header("Authorization", Http.basicAuth(oauthClientId, oauthClientSecret))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    try {
      HttpResponse<String> response =
          http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      JsonNode json = MAPPER.readTree(response.body());
      long expiresIn = json.path("expires_in").asLong();
      log.info("got access_token, expires in {} seconds", expiresIn);
      cachedOauthToken = json.path("access_token").asText();
      cachedOauthExpiry = System.currentTimeMillis() / 1000 + expiresIn - 10;
    } catch (Exception e) {
      log.warn("OAuth server connection error: {}", e.toString());
      throw new IllegalStateException(e);
    }
  }

  // -- requests ------------------------------------------------------------

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
              "OPA Client: unexpected status code: "
                  + response.statusCode()
                  + ", error: "
                  + response.body());
        }
        return response;
      } catch (Exception e) {
        last = e;
        log.warn("Opa connection error: {}", e.toString());
        sleepBeforeRetry(attempt);
      }
    }
    throw new IllegalStateException("OPA request failed", last);
  }

  private void sleepBeforeRetry(int attempt) {
    try {
      Thread.sleep(retry.waitMillis(attempt, java.util.concurrent.ThreadLocalRandom.current()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // -- policies ------------------------------------------------------------

  @Override
  public void setPolicy(String policyId, String policyCode, String transactionId) {
    if (IgnorePaths.shouldIgnorePath(policyId, pathsToIgnore)) {
      log.info(
          "Ignoring setting policy - {}, set in POLICY_STORE_POLICY_PATHS_TO_IGNORE.", policyId);
      return;
    }
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + policyId))
            .header("content-type", "text/plain")
            .PUT(HttpRequest.BodyPublishers.ofString(policyCode, StandardCharsets.UTF_8)),
        List.of(200, 400));
  }

  /** Used by the postponed-retry loop, which needs the status rather than an exception. */
  private Answer attemptSetPolicy(String policyId, String policyCode) {
    if (IgnorePaths.shouldIgnorePath(policyId, pathsToIgnore)) {
      return new Answer(200, "");
    }
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + policyId))
                .header("content-type", "text/plain")
                .PUT(HttpRequest.BodyPublishers.ofString(policyCode, StandardCharsets.UTF_8)),
            List.of(200, 400));
    return new Answer(response.statusCode(), response.body());
  }

  private Answer attemptDeletePolicy(String policyId) {
    if (IgnorePaths.shouldIgnorePath(policyId, pathsToIgnore)) {
      return new Answer(200, "");
    }
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + policyId)).DELETE(),
            List.of(200, 404));
    return new Answer(response.statusCode(), response.body());
  }

  @Override
  public String getPolicy(String policyId) {
    try {
      HttpResponse<String> response =
          send(HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + policyId)).GET(), null);
      JsonNode json = MAPPER.readTree(response.body());
      JsonNode raw = json.path("result").path("raw");
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
      return extractModulesFromPoliciesJson(
          MAPPER.readTree(response.body()), healthCheckPolicyPath);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * R79: what the store already holds, less the health-check policy and less anything whose
   * package begins with {@code system.} — but a module with no parsable package is kept, because
   * an unparsable module is still one OPAL put there and still one it has to be able to delete.
   */
  public static Map<String, String> extractModulesFromPoliciesJson(
      JsonNode result, String healthCheckPolicyPath) {
    Map<String, String> modules = new LinkedHashMap<>();
    for (JsonNode policy : result.path("result")) {
      JsonNode id = policy.get("id");
      if (id == null || id.isNull()) {
        continue;
      }
      String raw = policy.path("raw").asText("");
      String packageName = Rego.getRegoPackage(raw);
      if (packageName != null && packageName.startsWith("system.")) {
        continue;
      }
      if (id.asText().equals(healthCheckPolicyPath)) {
        continue;
      }
      modules.put(id.asText(), raw);
    }
    return modules;
  }

  @Override
  public void deletePolicy(String policyId, String transactionId) {
    if (IgnorePaths.shouldIgnorePath(policyId, pathsToIgnore)) {
      log.info(
          "Ignoring deleting policy - {}, set in POLICY_STORE_POLICY_PATHS_TO_IGNORE.", policyId);
      return;
    }
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/policies/" + policyId)).DELETE(),
        List.of(200, 404));
  }

  @Override
  public List<String> getPolicyModuleIds() {
    Map<String, String> modules = getPolicies();
    return modules == null ? List.of() : new ArrayList<>(modules.keySet());
  }

  @Override
  public String getPolicyVersion() {
    return policyVersion;
  }

  @Override
  public void setPolicies(Policy.PolicyBundle bundle, String transactionId) {
    if (bundle.old_hash() == null) {
      setPoliciesFromCompleteBundle(bundle);
    } else {
      setPoliciesFromDeltaBundle(bundle);
    }
  }

  /** R76: data first, then policies, then a delete of everything the bundle does not name. */
  private void setPoliciesFromCompleteBundle(Policy.PolicyBundle bundle) {
    Set<String> inStore = new LinkedHashSet<>(getPolicyModuleIds());
    Set<String> inBundle = new LinkedHashSet<>();
    for (Policy.RegoModule module : bundle.policy_modules()) {
      inBundle.add(module.path());
    }
    Set<String> toDelete = new LinkedHashSet<>(inStore);
    toDelete.removeAll(inBundle);

    lock.lock();
    try {
      for (Policy.DataModule module : BundleUtils.sortedDataModulesToLoad(bundle)) {
        setPolicyDataFromBundleDataModule(module, bundle.hash());
      }
      List<Operation> operations = new ArrayList<>();
      for (Policy.RegoModule module : BundleUtils.sortedPolicyModulesToLoad(bundle)) {
        operations.add(() -> attemptSetPolicy(module.path(), module.rego()));
      }
      attemptOperationsWithPostponedFailureRetry(operations);
      for (String moduleId : toDelete) {
        deletePolicy(moduleId, null);
      }
      policyVersion = bundle.hash();
    } finally {
      lock.unlock();
    }
  }

  /** R77: data writes, data deletes, then policy writes and policy deletes together. */
  private void setPoliciesFromDeltaBundle(Policy.PolicyBundle bundle) {
    lock.lock();
    try {
      for (Policy.DataModule module : BundleUtils.sortedDataModulesToLoad(bundle)) {
        setPolicyDataFromBundleDataModule(module, bundle.hash());
      }
      for (String moduleId : BundleUtils.sortedDataModulesToDelete(bundle)) {
        deletePolicyData(safeDataModulePath(moduleId), null);
      }
      List<Operation> operations = new ArrayList<>();
      for (Policy.RegoModule module : BundleUtils.sortedPolicyModulesToLoad(bundle)) {
        operations.add(() -> attemptSetPolicy(module.path(), module.rego()));
      }
      for (String moduleId : BundleUtils.sortedPolicyModulesToDelete(bundle)) {
        operations.add(() -> attemptDeletePolicy(moduleId));
      }
      attemptOperationsWithPostponedFailureRetry(operations);
      policyVersion = bundle.hash();
    } finally {
      lock.unlock();
    }
  }

  /**
   * One write or delete, returning what OPA answered.
   *
   * <p>The body as well as the status, because the status of a rejected policy is always 400 and
   * the body is the compiler error that says which rule would not parse. Without it an operator
   * sees a bare 400 for a bundle that will never load.
   */
  interface Operation {
    Answer run();
  }

  /** What OPA said about one write or delete. */
  record Answer(int status, String body) {}

  /**
   * R78: failures are retried at the end, repeatedly, until either none fails or all of them do.
   * That is what lets a renamed policy be written before the old one is deleted, and what lets a
   * policy that imports another be written whichever order the bundle put them in.
   */
  static void attemptOperationsWithPostponedFailureRetry(List<Operation> operations) {
    List<Operation> pending = operations;
    while (true) {
      List<Operation> failed = new ArrayList<>();
      List<String> failureMessages = new ArrayList<>();
      for (Operation operation : pending) {
        Answer answer = operation.run();
        if (answer.status() != 200) {
          failureMessages.add(
              "Failed policy operation. status: " + answer.status() + ", body: " + answer.body());
          failed.add(operation);
        }
      }
      if (failed.isEmpty()) {
        return;
      }
      if (failed.size() == pending.size()) {
        failureMessages.forEach(log::error);
        throw new IllegalStateException("Giving up setting / deleting failed modules to OPA");
      }
      pending = failed;
    }
  }

  // -- data ----------------------------------------------------------------

  /** R81: the empty path, {@code .} and absent are all the root; anything else gets a slash. */
  public static String safeDataModulePath(String path) {
    if (path == null || path.isEmpty() || path.equals(".")) {
      return "";
    }
    return path.startsWith("/") ? path : "/" + path;
  }

  private void setPolicyDataFromBundleDataModule(Policy.DataModule module, String hash) {
    String modulePath = safeDataModulePath(module.path());
    try {
      JsonNode data = MAPPER.readTree(module.data());
      setPolicyData(data, modulePath, null);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      log.warn(
          "bundle contains non-json data module: {} (bundle hash {})", modulePath, hash);
    }
  }

  @Override
  public void setPolicyData(JsonNode policyData, String path, String transactionId) {
    String safePath = safeDataModulePath(path);
    JsonNode data = policyData;
    // R51: OPA's root document must be an object, so a list written there is wrapped.
    if (safePath.isEmpty() && data != null && data.isArray()) {
      log.warn(
          "OPAL client was instructed to put a list on OPA's root document. In OPA the root"
              + " document must be an object so the original value was wrapped.");
      ObjectNode wrapper = MAPPER.createObjectNode();
      wrapper.set("items", data);
      data = wrapper;
    }
    String body = data == null ? "null" : data.toString();
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/data" + safePath))
            .header("content-type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)),
        List.of(204, 304));
    if (dataCache != null) {
      dataCache.set(safePath, data);
    }
  }

  @Override
  public void patchPolicyData(List<Store.JSONPatchAction> actions, String path,
      String transactionId) {
    String safePath = safeDataModulePath(path);
    JsonNode patch = excludeNoneFields(actions);
    // R258: OPA's root document must be an object, so what is sent there is wrapped — including
    // a patch, which is always a list. A patch written at the root therefore arrives as an object
    // with the actions under `items`, which is what the source sends and what a store reading
    // this rebuild's traffic will see.
    if (safePath.isEmpty() && patch.isArray()) {
      log.warn(
          "OPAL client was instructed to put a list on OPA's root document. In OPA the root"
              + " document must be an object so the original value was wrapped.");
      ObjectNode wrapper = MAPPER.createObjectNode();
      wrapper.set("items", patch);
      patch = wrapper;
    }
    String body = patch.toString();
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/data" + safePath))
            .header("Content-Type", "application/json-patch+json")
            .method("PATCH", HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)),
        List.of(204, 304));
    if (dataCache != null) {
      dataCache.patch(safePath, actions);
    }
  }

  /** OPAL strips nulls before sending, because OPA rejects a patch action carrying them. */
  static JsonNode excludeNoneFields(Object value) {
    JsonNode node = MAPPER.valueToTree(value);
    return strip(node);
  }

  private static JsonNode strip(JsonNode node) {
    if (node.isObject()) {
      ObjectNode object = (ObjectNode) node;
      List<String> toRemove = new ArrayList<>();
      object.fieldNames().forEachRemaining(name -> {
        if (object.get(name).isNull()) {
          toRemove.add(name);
        }
      });
      toRemove.forEach(object::remove);
      object.fields().forEachRemaining(entry -> strip(entry.getValue()));
    } else if (node.isArray()) {
      node.forEach(OpaClient::strip);
    }
    return node;
  }

  @Override
  public void deletePolicyData(String path, String transactionId) {
    String safePath = safeDataModulePath(path);
    // R57: an empty path is cleared by writing an empty object rather than by a delete.
    if (safePath.isEmpty()) {
      setPolicyData(MAPPER.createObjectNode(), "", transactionId);
      return;
    }
    send(
        HttpRequest.newBuilder(URI.create(apiUrl + "/data" + safePath)).DELETE(),
        List.of(204, 404));
    if (dataCache != null) {
      dataCache.delete(safePath);
    }
  }

  @Override
  public JsonNode getData(String path) {
    String safePath = path == null || path.isEmpty() || path.startsWith("/") ? path : "/" + path;
    try {
      HttpResponse<String> response =
          send(
              HttpRequest.newBuilder(URI.create(apiUrl + "/data" + (safePath == null ? "" : safePath)))
                  .GET(),
              null);
      JsonNode json = MAPPER.readTree(response.body());
      JsonNode result = json.get("result");
      return result == null ? MAPPER.createObjectNode() : result;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public JsonNode getDataWithInput(String path, JsonNode input) {
    String stripped = path.startsWith("/") ? path.substring(1) : path;
    ObjectNode body = MAPPER.createObjectNode();
    body.set("input", input);
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(URI.create(apiUrl + "/data/" + stripped))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8)),
            null);
    try {
      return MAPPER.readTree(response.body());
    } catch (Exception e) {
      return MAPPER.createObjectNode();
    }
  }

  // -- transaction log -----------------------------------------------------

  @Override
  public void initHealthcheckPolicy(String policyId, String policyCode) {
    transactionWriter = new TransactionLogPolicyWriter(this, policyId, policyCode);
    transactionWriter.persist(state);
  }

  /** R90: a failure to write the log into the engine is logged and otherwise ignored. */
  @Override
  public void logTransaction(Store.StoreTransaction transaction) {
    state.processTransaction(transaction);
    TransactionLogPolicyWriter writer = transactionWriter;
    if (writer != null) {
      try {
        writer.persist(state);
      } catch (Exception e) {
        log.error(
            "Cannot write to OPAL transaction log, transaction id={}, error={}",
            transaction.id(),
            e.toString());
      }
    }
  }

  @Override
  public boolean isReady() {
    return state.ready();
  }

  @Override
  public boolean isHealthy() {
    return state.healthy();
  }

  // -- offline backup ------------------------------------------------------

  @Override
  public String fullExport() {
    ObjectNode out = MAPPER.createObjectNode();
    out.set("policies", MAPPER.valueToTree(getPolicies()));
    out.set("data", dataCache == null ? MAPPER.createObjectNode() : dataCache.getData());
    return out.toString();
  }

  @Override
  public void fullImport(String contents) {
    try {
      JsonNode imported = MAPPER.readTree(contents);
      List<Operation> operations = new ArrayList<>();
      JsonNode policies = imported.path("policies");
      policies
          .fieldNames()
          .forEachRemaining(
              id -> operations.add(() -> attemptSetPolicy(id, policies.get(id).asText())));
      attemptOperationsWithPostponedFailureRetry(operations);
      setPolicyData(imported.path("data"), "", null);
    } catch (Exception e) {
      throw new IllegalStateException("could not import policy store backup", e);
    }
  }
}
