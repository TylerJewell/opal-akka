package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import java.util.List;
import java.util.Map;

/**
 * What OPAL needs of a policy engine — SPEC-002 R76 to R91.
 *
 * <p>Two engines implement this (OPA and Cedar) plus an in-memory stand-in, and the client is
 * written against the interface, so which engine is behind it is a configuration choice.
 */
public interface PolicyStoreClient {

  void setPolicy(String policyId, String policyCode, String transactionId);

  String getPolicy(String policyId);

  Map<String, String> getPolicies();

  void deletePolicy(String policyId, String transactionId);

  List<String> getPolicyModuleIds();

  void setPolicies(Policy.PolicyBundle bundle, String transactionId);

  String getPolicyVersion();

  void setPolicyData(JsonNode policyData, String path, String transactionId);

  void patchPolicyData(List<Store.JSONPatchAction> actions, String path, String transactionId);

  void deletePolicyData(String path, String transactionId);

  JsonNode getData(String path);

  /**
   * R370: a document evaluated against an input, with the engine's own answer proxied back.
   *
   * <p>The status and the headers are part of the answer and not packaging around it: a query
   * that names a document the engine does not have answers 200 with an empty body, and one whose
   * input will not parse answers 400 with a body that says which field. A caller handed only the
   * parsed body cannot tell those apart.
   */
  Proxied getDataWithInput(String path, JsonNode input);

  /** What the engine answered, as it answered it. */
  record Proxied(int status, java.util.Map<String, String> headers, String body) {

    public JsonNode json() {
      try {
        return io.akka.opal.server.pubsub.Rpc.MAPPER.readTree(body);
      } catch (Exception e) {
        return io.akka.opal.server.pubsub.Rpc.MAPPER.createObjectNode();
      }
    }
  }

  void initHealthcheckPolicy(String policyId, String policyCode);

  void logTransaction(Store.StoreTransaction transaction);

  boolean isReady();

  boolean isHealthy();

  /** Writes everything the store holds, for offline mode's backup file. */
  String fullExport();

  /** Loads a backup back in. */
  void fullImport(String contents);

  default void close() {}
}
