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

  public PolicyStoreClient store() {
    return store;
  }

  /** Records that a method ran as part of this transaction. */
  public void action(String name) {
    actions.add(name);
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
