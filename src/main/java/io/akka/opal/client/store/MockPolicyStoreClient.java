package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.util.JsonPatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * The in-memory store — {@code POLICY_STORE_TYPE=MOCK}.
 *
 * <p>It is part of the product rather than of its tests: a deployment that wants OPAL's
 * propagation without an engine behind it selects this, and OPAL's own integration tests drive
 * the whole client through it. Policies are accepted and discarded; data is kept, keyed by the
 * destination path, and health is simply whether any data has arrived.
 */
public final class MockPolicyStoreClient implements PolicyStoreClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ObjectNode data = MAPPER.createObjectNode();
  private final CountDownLatch hasData = new CountDownLatch(1);

  @Override
  public void setPolicy(String policyId, String policyCode, String transactionId) {}

  @Override
  public String getPolicy(String policyId) {
    return null;
  }

  @Override
  public Map<String, String> getPolicies() {
    return Map.of();
  }

  @Override
  public void deletePolicy(String policyId, String transactionId) {}

  @Override
  public List<String> getPolicyModuleIds() {
    return List.of();
  }

  @Override
  public void setPolicies(Policy.PolicyBundle bundle, String transactionId) {}

  @Override
  public String getPolicyVersion() {
    return null;
  }

  @Override
  public synchronized void setPolicyData(JsonNode policyData, String path, String transactionId) {
    data.set(path == null ? "" : path, policyData);
    hasData.countDown();
  }

  @Override
  public synchronized void patchPolicyData(
      List<Store.JSONPatchAction> actions, String path, String transactionId) {
    ArrayNode document = MAPPER.createArrayNode();
    for (Store.JSONPatchAction action : actions) {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("op", action.op());
      node.put("path", "/".equals(path) ? action.path() : path + action.path());
      if (action.value() != null) {
        node.set("value", MAPPER.valueToTree(action.value()));
      }
      if (action.from() != null) {
        node.put("from", action.from());
      }
      document.add(node);
    }
    JsonNode result = JsonPatch.apply(data, document);
    data.removeAll();
    if (result != null && result.isObject()) {
      data.setAll((ObjectNode) result);
    }
    hasData.countDown();
  }

  /**
   * R371: a path this store never wrote is an error, not an empty answer.
   *
   * <p>The mock stands in for a real engine in a deployment that has no engine, and a test that
   * asks for a document it never wrote has a mistake in it. Answering nothing hides the mistake
   * inside whatever asserts on the result.
   */
  @Override
  public synchronized void deletePolicyData(String path, String transactionId) {
    if (path == null || path.isEmpty()) {
      data.removeAll();
    } else {
      requirePresent(path);
      data.remove(path);
    }
  }

  @Override
  public synchronized JsonNode getData(String path) {
    if (path == null || path.isEmpty()) {
      return data;
    }
    requirePresent(path);
    return data.get(path);
  }

  private void requirePresent(String path) {
    if (!data.has(path)) {
      throw new java.util.NoSuchElementException(path);
    }
  }

  @Override
  public Proxied getDataWithInput(String path, JsonNode input) {
    return new Proxied(200, java.util.Map.of(), "{}");
  }

  @Override
  public void initHealthcheckPolicy(String policyId, String policyCode) {}

  @Override
  public void logTransaction(Store.StoreTransaction transaction) {}

  @Override
  public boolean isReady() {
    return hasData.getCount() == 0;
  }

  @Override
  public boolean isHealthy() {
    return hasData.getCount() == 0;
  }

  /** Blocks until something has been written, which is how OPAL's own tests synchronise. */
  public void waitForData() throws InterruptedException {
    hasData.await();
  }

  @Override
  public synchronized String fullExport() {
    ObjectNode out = MAPPER.createObjectNode();
    out.set("policies", MAPPER.createObjectNode());
    out.set("data", data.deepCopy());
    return out.toString();
  }

  @Override
  public synchronized void fullImport(String contents) {
    try {
      JsonNode imported = MAPPER.readTree(contents);
      data.removeAll();
      JsonNode restored = imported.path("data");
      if (restored.isObject()) {
        data.setAll((ObjectNode) restored);
      }
      hasData.countDown();
    } catch (Exception e) {
      throw new IllegalStateException("could not import policy store backup", e);
    }
  }

  /** Every path written so far, in the order they arrived. */
  public synchronized List<String> writtenPaths() {
    List<String> out = new ArrayList<>();
    data.fieldNames().forEachRemaining(out::add);
    return out;
  }
}
