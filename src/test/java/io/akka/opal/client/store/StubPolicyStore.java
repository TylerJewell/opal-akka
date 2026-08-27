package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import java.util.List;
import java.util.Map;

/**
 * A policy store that accepts everything and remembers only what a test asks it to.
 *
 * <p>Standing in for the engine is fair wherever the claim under test is about what the client
 * decides rather than about what the engine does with it — which store to write to, which bundle
 * to ask for, which entries to skip. Where the claim is about the writing itself, the tests use a
 * real HTTP engine instead.
 */
public class StubPolicyStore implements PolicyStoreClient {

  private String policyVersion;

  public void setPolicyVersion(String version) {
    this.policyVersion = version;
  }

  @Override
  public String getPolicyVersion() {
    return policyVersion;
  }

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
  public void setPolicyData(JsonNode policyData, String path, String transactionId) {}

  @Override
  public void patchPolicyData(
      List<Store.JSONPatchAction> actions, String path, String transactionId) {}

  @Override
  public void deletePolicyData(String path, String transactionId) {}

  @Override
  public JsonNode getData(String path) {
    return null;
  }

  @Override
  public Proxied getDataWithInput(String path, JsonNode input) {
    return null;
  }

  @Override
  public void initHealthcheckPolicy(String policyId, String policyCode) {}

  @Override
  public void logTransaction(Store.StoreTransaction transaction) {}

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public boolean isHealthy() {
    return true;
  }

  @Override
  public String fullExport() {
    return "{}";
  }

  @Override
  public void fullImport(String contents) {}
}
