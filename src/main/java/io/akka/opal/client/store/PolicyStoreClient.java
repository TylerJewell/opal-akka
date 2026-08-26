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

  JsonNode getDataWithInput(String path, JsonNode input);

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
