package io.akka.opal.common.sources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.eclipse.jgit.lib.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where policy comes from, and how a change in it is noticed — SPEC-002 R37 and R43.
 *
 * <p>Polling runs only when the interval is positive, and an exception in one round does not stop
 * the loop: a repository that is unreachable for a minute must not leave the watcher dead for the
 * life of the process.
 */
public abstract class PolicySource {

  private static final Logger log = LoggerFactory.getLogger(PolicySource.class);

  private final List<BiConsumer<ObjectId, ObjectId>> onNewPolicy = new ArrayList<>();
  private final List<Consumer<Exception>> onFailure = new ArrayList<>();
  private final int pollingInterval;

  protected final String remoteSourceUrl;
  protected final String localClonePath;

  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> pollingTask;

  protected PolicySource(String remoteSourceUrl, String localClonePath, int pollingInterval) {
    this.remoteSourceUrl = remoteSourceUrl;
    this.localClonePath = io.akka.opal.common.git.RepoCloner.expandUser(localClonePath);
    this.pollingInterval = pollingInterval;
  }

  public void addOnNewPolicyCallback(BiConsumer<ObjectId, ObjectId> callback) {
    onNewPolicy.add(callback);
  }

  public void addOnFailureCallback(Consumer<Exception> callback) {
    onFailure.add(callback);
  }

  public abstract void getInitialPolicyStateFromRemote();

  public abstract void checkForChanges();

  /**
   * Lets go of whatever the source holds open. A git repository keeps its pack files mapped, and
   * a source that is stopped and not released leaves them mapped for the life of the process —
   * which on a scope-syncing server is one handle per scope that has ever been refreshed.
   */
  protected void releaseRepository() {}

  public void run() {
    getInitialPolicyStateFromRemote();
    if (pollingInterval > 0) {
      log.info("Launching polling task, interval: {} seconds", pollingInterval);
      startPollingTask();
    } else {
      log.info("Polling task is off");
    }
  }

  private synchronized void startPollingTask() {
    if (pollingTask != null) {
      return;
    }
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-policy-poller");
              thread.setDaemon(true);
              return thread;
            });
    pollingTask =
        scheduler.scheduleWithFixedDelay(
            () -> {
              try {
                checkForChanges();
              } catch (Exception e) {
                log.error("Error occurred during polling task check_for_changes: {}", e.toString());
              }
            },
            // R320: the first check runs immediately. The source performs the check and then
            // sleeps, so a commit landing seconds after start-up is seen at once rather than a
            // whole polling interval later.
            0,
            pollingInterval,
            TimeUnit.SECONDS);
  }

  public synchronized void stop() {
    releaseRepository();
    if (pollingTask != null) {
      pollingTask.cancel(true);
      pollingTask = null;
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
  }

  protected void fireNewPolicy(ObjectId oldCommit, ObjectId newCommit) {
    for (BiConsumer<ObjectId, ObjectId> callback : onNewPolicy) {
      callback.accept(oldCommit, newCommit);
    }
  }

  protected void fireFailure(Exception exception) {
    for (Consumer<Exception> callback : onFailure) {
      callback.accept(exception);
    }
  }
}
