package io.akka.opal.client.data;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.store.PolicyStoreClient;
import io.akka.opal.client.store.StoreTransactionContext;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.sync.HierarchicalLock;
import io.akka.opal.common.util.Hashing;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applying a data update to the policy store — SPEC-002 R46 to R55.
 *
 * <p>Entries are processed in order, each under a lock on its destination path, so two updates
 * writing to overlapping paths cannot interleave. Entries writing to unrelated paths do not wait
 * for each other, which is the whole reason the lock is hierarchical rather than global.
 */
public final class DataUpdater {

  private static final Logger log = LoggerFactory.getLogger(DataUpdater.class);

  /** R55: the reason a repeating entry's own update carries, which callers see in their reports. */
  public static final String PERIODIC_UPDATE_REASON = "Periodic Update";

  private final PolicyStoreClient store;
  private final DataFetcher fetcher;
  private final CallbacksRegister callbacksRegister;
  private final CallbacksReporter callbacksReporter;
  private final List<String> dataTopics;
  private final boolean shouldSendReports;
  private final boolean splitRootData;
  private final HierarchicalLock destinationLock = new HierarchicalLock();

  /**
   * R296: where an update runs, which is never the thread it arrived on.
   *
   * <p>The source dispatches each update as its own task, so several run at once and the pub/sub
   * reader is free to take the next frame. Running one inline on the socket's thread would stall
   * every later frame on that channel behind it — including the keep-alive — and a slow data
   * source would look like a dropped connection.
   */
  private final java.util.concurrent.ExecutorService updates =
      java.util.concurrent.Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "opal-data-update");
            thread.setDaemon(true);
            return thread;
          });

  private final java.util.concurrent.atomic.AtomicInteger inFlight =
      new java.util.concurrent.atomic.AtomicInteger();

  /** Runs an update off the caller's thread. */
  public void triggerDataUpdate(Data.DataUpdate update) {
    inFlight.incrementAndGet();
    updates.execute(
        () -> {
          try {
            updatePolicyData(update);
          } catch (Exception e) {
            log.error("Failed to update policy data", e);
          } finally {
            inFlight.decrementAndGet();
          }
        });
  }

  /** Waits for the updates in flight, which is what a test asserting on the effect needs. */
  public void awaitIdle(java.time.Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline && inFlight.get() > 0) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private final List<ScheduledFuture<?>> pollingTasks = new ArrayList<>();
  private ScheduledExecutorService scheduler;

  public DataUpdater(
      PolicyStoreClient store,
      DataFetcher fetcher,
      CallbacksRegister callbacksRegister,
      CallbacksReporter callbacksReporter,
      List<String> dataTopics,
      boolean shouldSendReports,
      boolean splitRootData) {
    this.store = store;
    this.fetcher = fetcher;
    this.callbacksRegister = callbacksRegister;
    this.callbacksReporter = callbacksReporter;
    this.dataTopics = dataTopics;
    this.shouldSendReports = shouldSendReports;
    this.splitRootData = splitRootData;
  }

  /** R46: an entry reaches this client when its topics and the client's intersect. */
  public void updatePolicyData(Data.DataUpdate update) {
    List<Data.DataEntryReport> reports = new ArrayList<>();
    Set<String> subscribed = new LinkedHashSet<>(dataTopics);

    for (Data.DataSourceEntry entry : update.entries()) {
      if (entry.topics() == null || entry.topics().isEmpty()) {
        log.debug(
            "Data entry for url {} has no topics, skipping",
            io.akka.opal.common.util.Urls.redactUrl(entry.url()));
        continue;
      }
      boolean matches = false;
      for (String topic : entry.topics()) {
        if (subscribed.contains(topic)) {
          matches = true;
          break;
        }
      }
      if (!matches) {
        log.debug(
            "Data entry for url {} has no topics matching the data topics, skipping",
            io.akka.opal.common.util.Urls.redactUrl(entry.url()));
        continue;
      }
      reports.add(processEntry(update, entry));
    }
    sendReports(reports, update);
  }

  private Data.DataEntryReport processEntry(Data.DataUpdate update, Data.DataSourceEntry entry) {
    StoreTransactionContext transaction =
        new StoreTransactionContext(store, update.id(), Store.TransactionType.data);
    try {
      destinationLock.acquire(entry.dst_path());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Data.DataEntryReport(entry, false, false, null);
    }
    try (transaction) {
      return fetchAndSaveData(entry, transaction);
    } finally {
      destinationLock.release(entry.dst_path());
    }
  }

  /** R53: a failed fetch has no hash; a fetch that succeeded and failed to save keeps one. */
  Data.DataEntryReport fetchAndSaveData(
      Data.DataSourceEntry entry, StoreTransactionContext transaction) {
    JsonNode result;
    try {
      result = fetchData(entry);
    } catch (Exception e) {
      transaction.updateRemoteStatus(
          io.akka.opal.common.util.Urls.redactUrl(entry.url()),
          false,
          io.akka.opal.common.util.Urls.redactUrlInText(e.toString(), entry.url()));
      transaction.fail(e);
      return new Data.DataEntryReport(entry, false, false, null);
    }
    String hash = Hashing.calcHash(result);
    try {
      storeFetchedData(entry, result, transaction);
    } catch (Exception e) {
      log.error("Failed to save data update to policy-store", e);
      transaction.updateRemoteStatus(
          io.akka.opal.common.util.Urls.redactUrl(entry.url()),
          false,
          "Failed to save data to policy store: "
              + io.akka.opal.common.util.Urls.redactUrlInText(e.toString(), entry.url()));
      transaction.fail(e);
      return new Data.DataEntryReport(entry, true, false, hash);
    }
    transaction.updateRemoteStatus(io.akka.opal.common.util.Urls.redactUrl(entry.url()), true, "");
    return new Data.DataEntryReport(entry, true, true, hash);
  }

  JsonNode fetchData(Data.DataSourceEntry entry) {
    JsonNode result = fetcher.handleUrl(entry.url(), entry.config(), entry.data());
    if (result == null) {
      throw new IllegalStateException(
          "Fetched data is empty for entry " + io.akka.opal.common.util.Urls.redactUrl(entry.url()));
    }
    return result;
  }

  /** R49, R50: the destination is normalised, and the root may be split key by key. */
  void storeFetchedData(
      Data.DataSourceEntry entry, JsonNode result, StoreTransactionContext transaction) {
    String policyStorePath = entry.dst_path() == null ? "" : entry.dst_path();
    if (!policyStorePath.isEmpty() && !policyStorePath.startsWith("/")) {
      policyStorePath = "/" + policyStorePath;
    }
    if (splitRootData
        && (policyStorePath.equals("/") || policyStorePath.isEmpty())
        && result.isObject()) {
      log.info("Splitting root data to {} keys", result.size());
      for (Iterator<Map.Entry<String, JsonNode>> it = result.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> field = it.next();
        setPolicyData(transaction, "/" + field.getKey(), entry.save_method(), field.getValue());
      }
      return;
    }
    setPolicyData(transaction, policyStorePath, entry.save_method(), result);
  }

  private void setPolicyData(
      StoreTransactionContext transaction, String path, String saveMethod, JsonNode data) {
    log.info("Saving fetched data to policy-store: destination path='{}'",
        path.isEmpty() ? "/" : path);
    if ("PUT".equals(saveMethod)) {
      transaction.store().setPolicyData(data, path, transaction.transactionId());
      return;
    }
    List<Store.JSONPatchAction> actions = new ArrayList<>();
    for (JsonNode node : data) {
      actions.add(Rpc.MAPPER.convertValue(node, Store.JSONPatchAction.class));
    }
    transaction.store().patchPolicyData(actions, path, transaction.transactionId());
  }

  /** R54: reports go out only when the client was asked to send them. */
  void sendReports(List<Data.DataEntryReport> reports, Data.DataUpdate update) {
    if (!shouldSendReports) {
      return;
    }
    Data.DataUpdateReport whole =
        new Data.DataUpdateReport(update.id(), reports, null, Map.of());
    List<CallbacksRegister.CallbackConfig> extra =
        callbacksRegister.normalizeCallbacks(update.callback().callbacks());
    callbacksReporter.reportUpdateResults(whole, extra);
  }

  /**
   * R55: an entry carrying a polling interval is not part of the initial load; it becomes its own
   * repeating update. Reconnecting cancels the previous set before starting a new one, or every
   * reconnect would leave another copy of every timer behind.
   */
  public synchronized void getBasePolicyData(
      Data.DataSourceConfig sourcesConfig, String reason) {
    log.info("Performing data configuration, reason: {}", reason);
    stopPollingUpdateTasks();

    List<Data.DataSourceEntry> initial = new ArrayList<>();
    List<Data.DataSourceEntry> periodic = new ArrayList<>();
    for (Data.DataSourceEntry entry : sourcesConfig.entries()) {
      if (entry.periodic_update_interval() != null) {
        periodic.add(entry);
      } else {
        initial.add(entry);
      }
    }
    updatePolicyData(new Data.DataUpdate(null, initial, reason, null));

    if (periodic.isEmpty()) {
      return;
    }
    scheduler =
        Executors.newScheduledThreadPool(
            Math.min(4, periodic.size()),
            runnable -> {
              Thread thread = new Thread(runnable, "opal-periodic-data");
              thread.setDaemon(true);
              return thread;
            });
    for (Data.DataSourceEntry entry : periodic) {
      long millis = (long) (entry.periodic_update_interval() * 1000);
      pollingTasks.add(
          scheduler.scheduleWithFixedDelay(
              () -> {
                try {
                  updatePolicyData(
                      new Data.DataUpdate(null, List.of(entry), PERIODIC_UPDATE_REASON, null));
                } catch (Exception e) {
                  log.error("periodic data update failed: {}", e.toString());
                }
              },
              // R295: the first call is not delayed. The source's repeated call performs the work
              // and then sleeps, so an entry with an hour's interval is loaded at start-up rather
              // than an hour after it — and again after every reconnection.
              0,
              millis,
              TimeUnit.MILLISECONDS));
    }
  }

  public synchronized void stopPollingUpdateTasks() {
    for (ScheduledFuture<?> task : pollingTasks) {
      task.cancel(true);
    }
    pollingTasks.clear();
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
  }
}
