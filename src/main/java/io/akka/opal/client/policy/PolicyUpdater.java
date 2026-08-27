package io.akka.opal.client.policy;

import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.store.PolicyStoreClient;
import io.akka.opal.client.store.StoreTransactionContext;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeping the policy store's policy in step with the server — SPEC-002 R92 to R94.
 *
 * <p>An update names the directories that changed; this client fetches only the intersection with
 * the ones it subscribed to. A client watching {@code envs/prod} does not re-read {@code envs/dev}
 * because somebody committed there, which is the point of the directory topics.
 */
public final class PolicyUpdater {

  private static final Logger log = LoggerFactory.getLogger(PolicyUpdater.class);

  private final PolicyStoreClient store;
  private final PolicyFetcher fetcher;
  private final CallbacksReporter callbacksReporter;
  private final List<String> subscriptionDirectories;
  private final boolean shouldSendReports;

  /**
   * R294: what a caller asked for, waiting for the one consumer that performs it.
   *
   * <p>Every trigger — a message from the server, this client's own route, an engine that has
   * just restarted — goes on here and returns. One thread drains it, so two updates never
   * interleave writes into the store, and a failure is logged by the consumer rather than
   * escaping to whoever asked. That is why {@code POST /policy-updater/trigger} answers 200
   * before the bundle has been fetched, and answers 200 even when the fetch fails.
   */
  private final java.util.concurrent.BlockingQueue<Request> queue =
      new java.util.concurrent.LinkedBlockingQueue<>();

  /**
   * Requests asked for and not yet finished — raised before the request reaches the queue and
   * lowered after the work is done, so there is no instant at which a request is in neither.
   */
  private final java.util.concurrent.atomic.AtomicInteger pending =
      new java.util.concurrent.atomic.AtomicInteger();

  private final Thread consumer;

  private volatile boolean running = true;

  private record Request(List<String> directories, boolean forceFullUpdate) {}

  public PolicyUpdater(
      PolicyStoreClient store,
      PolicyFetcher fetcher,
      CallbacksReporter callbacksReporter,
      List<String> subscriptionDirectories,
      boolean shouldSendReports) {
    this.store = store;
    this.fetcher = fetcher;
    this.callbacksReporter = callbacksReporter;
    this.subscriptionDirectories = subscriptionDirectories;
    this.shouldSendReports = shouldSendReports;
    this.consumer = new Thread(this::drain, "opal-policy-updates");
    this.consumer.setDaemon(true);
    this.consumer.start();
  }

  private void drain() {
    while (running) {
      Request request;
      try {
        request = queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      try {
        updatePolicy(request.directories(), request.forceFullUpdate());
      } catch (Exception e) {
        log.error("Failed to update policy", e);
      } finally {
        pending.decrementAndGet();
      }
    }
  }

  /** Stops the consumer. Anything still queued is dropped, as it is when the source stops. */
  public void close() {
    running = false;
    consumer.interrupt();
  }

  /** Waits for the queue to drain, which is what a test asserting on the effect needs. */
  public void awaitIdle(java.time.Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (pending.get() == 0) {
        return;
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** R94: only the directories this client watches are asked for. */
  public void onPolicyUpdateMessage(Policy.PolicyUpdateMessage message) {
    onPolicyUpdateMessage(null, message);
  }

  /** The same, with the topic it arrived on, which the source names in its own line. */
  public void onPolicyUpdateMessage(String topic, Policy.PolicyUpdateMessage message) {
    if (message == null) {
      log.warn("got policy update message without data, skipping policy update!");
      return;
    }
    log.info(
        "Received policy update: topic={}, message={}",
        topic,
        io.akka.opal.common.util.Repr.python(message));
    Set<String> intersection = new LinkedHashSet<>(message.changed_directories());
    intersection.retainAll(subscriptionDirectories);
    triggerUpdatePolicy(new ArrayList<>(intersection), false);
  }

  /**
   * R93, R259 and R260: a differential bundle against what the store holds, or a complete one.
   *
   * <p>One at a time. Three separate things trigger an update — a message from the server, a call
   * on this client's own route, and an engine that has just restarted and needs its policy back —
   * and two of them running together interleave writes into one store, so the store ends holding
   * half of each bundle.
   *
   * <p>The directories are deduplicated first. A client subscribed to both {@code .} and
   * {@code ./svc} would otherwise fetch the second inside the first, and the bundle would carry
   * every file under {@code svc} twice.
   */
  public void triggerUpdatePolicy(List<String> directories, boolean forceFullUpdate) {
    pending.incrementAndGet();
    queue.add(new Request(directories, forceFullUpdate));
  }

  /** The work itself, on the consumer's thread. */
  synchronized void updatePolicy(List<String> directories, boolean forceFullUpdate) {
    doUpdatePolicy(directories, forceFullUpdate);
  }

  private void doUpdatePolicy(List<String> directories, boolean forceFullUpdate) {
    // R293: only the default set is deduplicated, and only an *absent* list falls back to it.
    // An empty list is a caller saying "no directories", which is what a policy update naming
    // directories this client does not subscribe to produces — and it asks for a bundle with no
    // path parameter at all rather than for everything this client subscribes to.
    List<String> effective =
        directories == null
            ? new ArrayList<>(
                io.akka.opal.common.util.Paths2.nonIntersectingDirectories(subscriptionDirectories))
            : new ArrayList<>(directories);
    String baseHash;
    if (forceFullUpdate) {
      log.info("full update was forced (ignoring stored hash if exists)");
      baseHash = null;
    } else {
      baseHash = store.getPolicyVersion();
    }
    if (baseHash == null) {
      log.info("Refetching policy code (full bundle)");
    } else {
      log.info("Refetching policy code (delta bundle), base hash: '{}'", baseHash);
    }

    Policy.PolicyBundle bundle = null;
    String bundleError = null;
    boolean bundleSucceeded = true;
    try {
      bundle = fetcher.fetchPolicyBundle(effective, baseHash);
      if (bundle != null && bundle.old_hash() == null) {
        // R362: the manifest is on both lines. It is the order the modules will be written in,
        // and a bundle that wrote them in the wrong order is diagnosed from this and nothing else.
        log.info(
            "Got policy bundle with {} rego files, {} data files, commit hash: '{}', manifest: {}",
            bundle.policy_modules().size(),
            bundle.data_modules().size(),
            bundle.hash(),
            bundle.manifest());
      } else if (bundle != null) {
        // And the deleted files are on the delta line: a differential bundle that removes a
        // module says so only here.
        log.info(
            "got policy bundle (delta): '{}' -> '{}', manifest: {}, deleted: {}",
            bundle.old_hash(),
            bundle.hash(),
            bundle.manifest(),
            bundle.deleted_files() == null
                ? null
                : io.akka.opal.common.util.Repr.python(bundle.deleted_files()));
      }
    } catch (Exception e) {
      bundleError = e.toString();
      bundleSucceeded = false;
    }

    String bundleHash = bundle == null ? null : bundle.hash();
    StoreTransactionContext transaction =
        new StoreTransactionContext(store, bundleHash, Store.TransactionType.policy);
    try (transaction) {
      transaction.updateRemoteStatus(
          fetcher.policyEndpointUrl(), bundleSucceeded, bundleError);
      if (bundle != null) {
        try {
          transaction.store().setPolicies(bundle, transaction.transactionId());
        } catch (Exception e) {
          transaction.fail(e);
          throw e;
        }
        if (shouldSendReports) {
          callbacksReporter.reportUpdateResults(
              new Data.DataUpdateReport(null, List.of(), bundle.hash(), null), null);
        }
      }
    }
  }
}
