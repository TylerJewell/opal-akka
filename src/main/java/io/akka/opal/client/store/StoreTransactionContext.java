package io.akka.opal.client.store;

import io.akka.opal.common.schemas.Store;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A series of writes recorded as one transaction, which the store logs when it ends.
 *
 * <p>What is being recorded is not the writes themselves but whether the round trip succeeded:
 * the transaction log is how {@code /healthy} and {@code /ready} answer, so an update that
 * fetched cleanly and failed to write has to end up here as a failure with its reason.
 */
public final class StoreTransactionContext implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(StoreTransactionContext.class);

  private final PolicyStoreClient store;
  private final String transactionId;
  private final Store.TransactionType transactionType;
  private final String creationTime;
  private final List<String> actions = new ArrayList<>();
  private final List<Store.RemoteStatus> remotesStatus = new ArrayList<>();

  private Throwable failure;

  public StoreTransactionContext(
      PolicyStoreClient store, String transactionId, Store.TransactionType transactionType) {
    this.store = store;
    this.transactionId =
        transactionId == null ? UUID.randomUUID().toString().replace("-", "") : transactionId;
    this.transactionType = transactionType;
    this.creationTime = Instant.now().toString();
  }

  public String transactionId() {
    return transactionId;
  }

  /**
   * R372: the store, wrapped so that every write made through it records itself.
   *
   * <p>The transaction's action list is what the log says was attempted, and a list assembled by
   * hand beside the calls is a list that goes stale the moment a call is added. Here the six
   * methods that carry a transaction id record their own names, so the list is derived from what
   * ran rather than from what somebody remembered to write down.
   */
  public PolicyStoreClient store() {
    return new Recording();
  }

  /** The store underneath, for a caller that is not writing inside this transaction. */
  public PolicyStoreClient rawStore() {
    return store;
  }

  /** Records that a method ran as part of this transaction. */
  public void action(String name) {
    actions.add(name);
  }

  /** What the transaction recorded, in the order it happened. */
  public List<String> actions() {
    return List.copyOf(actions);
  }

  /** Every method that takes a transaction id records its own name and passes that id on. */
  private final class Recording implements PolicyStoreClient {

    @Override
    public void setPolicy(String policyId, String policyCode, String transactionId) {
      action("set_policy");
      store.setPolicy(policyId, policyCode, transactionId());
    }

    @Override
    public void deletePolicy(String policyId, String transactionId) {
      action("delete_policy");
      store.deletePolicy(policyId, transactionId());
    }

    @Override
    public void setPolicies(io.akka.opal.common.schemas.Policy.PolicyBundle bundle,
        String transactionId) {
      action("set_policies");
      store.setPolicies(bundle, transactionId());
    }

    @Override
    public void setPolicyData(com.fasterxml.jackson.databind.JsonNode policyData, String path,
        String transactionId) {
      action("set_policy_data");
      store.setPolicyData(policyData, path, transactionId());
    }

    @Override
    public void patchPolicyData(List<Store.JSONPatchAction> actions, String path,
        String transactionId) {
      action("patch_policy_data");
      store.patchPolicyData(actions, path, transactionId());
    }

    @Override
    public void deletePolicyData(String path, String transactionId) {
      action("delete_policy_data");
      store.deletePolicyData(path, transactionId());
    }

    @Override
    public String getPolicyVersion() {
      return store.getPolicyVersion();
    }

    @Override
    public String getPolicy(String policyId) {
      return store.getPolicy(policyId);
    }

    @Override
    public java.util.Map<String, String> getPolicies() {
      return store.getPolicies();
    }

    @Override
    public List<String> getPolicyModuleIds() {
      return store.getPolicyModuleIds();
    }

    @Override
    public com.fasterxml.jackson.databind.JsonNode getData(String path) {
      return store.getData(path);
    }

    @Override
    public Proxied getDataWithInput(String path,
        com.fasterxml.jackson.databind.JsonNode input) {
      return store.getDataWithInput(path, input);
    }

    @Override
    public void initHealthcheckPolicy(String policyId, String policyCode) {
      store.initHealthcheckPolicy(policyId, policyCode);
    }

    @Override
    public void logTransaction(Store.StoreTransaction transaction) {
      store.logTransaction(transaction);
    }

    @Override
    public boolean isReady() {
      return store.isReady();
    }

    @Override
    public boolean isHealthy() {
      return store.isHealthy();
    }

    @Override
    public String fullExport() {
      return store.fullExport();
    }

    @Override
    public void fullImport(String content) {
      store.fullImport(content);
    }
  }

  public void updateRemoteStatus(String url, boolean succeed, String error) {
    remotesStatus.add(new Store.RemoteStatus(url, succeed, error));
  }

  public void fail(Throwable throwable) {
    this.failure = throwable;
  }

  @Override
  public void close() {
    List<Store.RemoteStatus> failedRemotes = new ArrayList<>();
    for (Store.RemoteStatus status : remotesStatus) {
      if (!status.succeed()) {
        failedRemotes.add(status);
      }
    }
    if (remotesStatus.isEmpty() && (transactionId == null || actions.isEmpty())) {
      return;
    }
    String endTime = Instant.now().toString();
    Store.StoreTransaction transaction;
    if (failure != null || !failedRemotes.isEmpty()) {
      String errorMessage;
      if (failure != null) {
        errorMessage = failure.toString();
      } else {
        List<String> networkErrors = new ArrayList<>();
        for (Store.RemoteStatus status : failedRemotes) {
          if (status.error() != null) {
            networkErrors.add(status.error());
          }
        }
        errorMessage = networkErrors.isEmpty() ? null : String.join(";", networkErrors);
      }
      transaction =
          new Store.StoreTransaction(
              transactionId,
              List.copyOf(actions),
              transactionType,
              false,
              errorMessage == null ? "" : errorMessage,
              creationTime,
              endTime,
              List.copyOf(remotesStatus));
      log.error(
          "OPA transaction failed, transaction id={}, actions={}, error={}",
          transactionId,
          actions,
          errorMessage);
    } else {
      transaction =
          new Store.StoreTransaction(
              transactionId,
              List.copyOf(actions),
              transactionType,
              true,
              null,
              creationTime,
              endTime,
              List.copyOf(remotesStatus));
    }
    store.logTransaction(transaction);
  }
}
