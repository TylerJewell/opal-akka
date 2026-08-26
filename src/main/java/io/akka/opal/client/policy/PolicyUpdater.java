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
  }

  /** R94: only the directories this client watches are asked for. */
  public void onPolicyUpdateMessage(Policy.PolicyUpdateMessage message) {
    if (message == null) {
      log.warn("got policy update message without data, skipping policy update!");
      return;
    }
    log.info(
        "Received policy update: old={}, new={}",
        message.old_policy_hash(),
        message.new_policy_hash());
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
  public synchronized void triggerUpdatePolicy(List<String> directories, boolean forceFullUpdate) {
    List<String> chosen =
        directories == null || directories.isEmpty() ? subscriptionDirectories : directories;
    List<String> effective =
        new ArrayList<>(io.akka.opal.common.util.Paths2.nonIntersectingDirectories(chosen));
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
        log.info(
            "Got policy bundle with {} rego files, {} data files, commit hash: '{}'",
            bundle.policy_modules().size(),
            bundle.data_modules().size(),
            bundle.hash());
      } else if (bundle != null) {
        log.info(
            "got policy bundle (delta): '{}' -> '{}', manifest: {}",
            bundle.old_hash(),
            bundle.hash(),
            bundle.manifest());
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
        transaction.action("set_policies");
        try {
          store.setPolicies(bundle, transaction.transactionId());
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
