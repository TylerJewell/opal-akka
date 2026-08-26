package io.akka.opal.client.store;

import io.akka.opal.common.schemas.Store;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whether the client has finished loading, and whether it is still working — SPEC-002 R87 to R89.
 *
 * <p>{@code engineReachable} starts true and is maintained by a background probe. Before the
 * first probe completes, health reflects only the transactions, which is what an operator sees
 * in the seconds after start-up; the probe then flips it either way as the engine comes and goes.
 */
public final class TransactionLogState {

  private static final Logger log = LoggerFactory.getLogger(TransactionLogState.class);

  private final boolean dataUpdaterDisabled;
  private final boolean policyUpdaterDisabled;

  private int successfulPolicyTransactions;
  private int failedPolicyTransactions;
  private int successfulDataTransactions;
  private int failedDataTransactions;

  private Store.StoreTransaction lastPolicyTransaction;
  private Store.StoreTransaction lastFailedPolicyTransaction;
  private Store.StoreTransaction lastDataTransaction;
  private Store.StoreTransaction lastFailedDataTransaction;

  private volatile boolean engineReachable = true;

  public TransactionLogState(boolean dataUpdaterEnabled, boolean policyUpdaterEnabled) {
    this.dataUpdaterDisabled = !dataUpdaterEnabled;
    this.policyUpdaterDisabled = !policyUpdaterEnabled;
  }

  /** R87: at least one policy transaction, and — unless data is off — at least one data one. */
  public synchronized boolean ready() {
    return successfulPolicyTransactions > 0
        && (dataUpdaterDisabled || successfulDataTransactions > 0);
  }

  public boolean engineReachable() {
    return engineReachable;
  }

  public void setEngineReachable(boolean value) {
    this.engineReachable = value;
  }

  /** R88 and R89: the last transaction of each kind succeeded, and the engine answers. */
  public synchronized boolean healthy() {
    boolean policyHealthy = lastPolicyTransaction != null && lastPolicyTransaction.success();
    boolean dataHealthy = lastDataTransaction != null && lastDataTransaction.success();
    boolean transactionsHealthy =
        (policyUpdaterDisabled || policyHealthy) && (dataUpdaterDisabled || dataHealthy);
    boolean isHealthy = transactionsHealthy && engineReachable;
    log.debug(
        "OPA client health: {} (policy: {}, data: {}, engine_reachable: {})",
        isHealthy,
        policyHealthy,
        dataHealthy,
        engineReachable);
    return isHealthy;
  }

  public synchronized void processTransaction(Store.StoreTransaction transaction) {
    if (transaction.transaction_type() == Store.TransactionType.policy) {
      if (transaction.success()) {
        lastPolicyTransaction = transaction;
        successfulPolicyTransactions++;
      } else {
        lastFailedPolicyTransaction = transaction;
        failedPolicyTransactions++;
      }
    } else if (transaction.transaction_type() == Store.TransactionType.data) {
      if (transaction.success()) {
        lastDataTransaction = transaction;
        successfulDataTransactions++;
      } else {
        lastFailedDataTransaction = transaction;
        failedDataTransactions++;
      }
    }
  }

  public synchronized Object lastPolicyTransaction() {
    return lastPolicyTransaction == null ? Map.of() : lastPolicyTransaction;
  }

  public synchronized Object lastFailedPolicyTransaction() {
    return lastFailedPolicyTransaction == null ? Map.of() : lastFailedPolicyTransaction;
  }

  public synchronized Object lastDataTransaction() {
    return lastDataTransaction == null ? Map.of() : lastDataTransaction;
  }

  public synchronized Object lastFailedDataTransaction() {
    return lastFailedDataTransaction == null ? Map.of() : lastFailedDataTransaction;
  }

  public synchronized Map<String, Integer> transactionPolicyStatistics() {
    Map<String, Integer> out = new LinkedHashMap<>();
    out.put("successful", successfulPolicyTransactions);
    out.put("failed", failedPolicyTransactions);
    return out;
  }

  public synchronized Map<String, Integer> transactionDataStatistics() {
    Map<String, Integer> out = new LinkedHashMap<>();
    out.put("successful", successfulDataTransactions);
    out.put("failed", failedDataTransactions);
    return out;
  }
}
