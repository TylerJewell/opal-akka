package io.akka.opal.server.scopes;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.git.PolicyUpdates;
import io.akka.opal.common.git.RepoCloner;
import io.akka.opal.common.metrics.Metrics;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.PolicySource;
import io.akka.opal.common.schemas.Scopes;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeping every scope's clone in step, and telling its clients when one moves — SPEC-002 R103,
 * R106, R111 and R234–R240.
 *
 * <p>A purge removes a clone only when no surviving scope still names that source. Two scopes can
 * share one clone, so removing it on the first delete would take the policy away from a tenant
 * that never asked for anything.
 *
 * <p>A sync is not only a fetch. A scope whose repository moved has clients waiting to be told,
 * and the topics they are told on carry the scope's own name in front — which is what keeps one
 * tenant's policy change from waking every other tenant's clients.
 */
public final class ScopesService {

  private static final Logger log = LoggerFactory.getLogger(ScopesService.class);

  /** What a purge names: the source whose clone may go, and the scope that triggered it. */
  public record ScopePurgeCommand(String source_id, String scope_id) {}

  private final ScopeRepository scopes;
  private final Path baseDir;
  private final int shards;
  private final List<String> policyExtensions;
  private final BiConsumer<List<String>, Object> publish;
  private final String purgeChannel;

  /** R240: the clone purges a delete started, so shutdown can wait for them. */
  private final List<Future<?>> localPurges = new CopyOnWriteArrayList<>();

  private volatile Metrics metrics = new Metrics();
  private volatile int maxConcurrentSyncs = 10;

  public ScopesService(
      ScopeRepository scopes,
      Path baseDir,
      int shards,
      List<String> policyExtensions,
      BiConsumer<List<String>, Object> publish) {
    this(scopes, baseDir, shards, policyExtensions, publish, "__opal_scope_purge__");
  }

  public ScopesService(
      ScopeRepository scopes,
      Path baseDir,
      int shards,
      List<String> policyExtensions,
      BiConsumer<List<String>, Object> publish,
      String purgeChannel) {
    this.scopes = scopes;
    this.baseDir = baseDir;
    this.shards = shards;
    this.policyExtensions = policyExtensions;
    this.publish = publish;
    this.purgeChannel = purgeChannel;
  }

  public void configure(Metrics registry, int concurrency) {
    if (registry != null) {
      this.metrics = registry;
    }
    this.maxConcurrentSyncs = Math.max(1, concurrency);
  }

  public GitPolicyFetcher fetcherFor(Scopes.Scope scope) {
    return new GitPolicyFetcher(
        baseDir, scope.scope_id(), scope.policy(), shards, policyExtensions);
  }

  public String sourceIdOf(PolicySource.GitPolicyScopeSource source) {
    return GitPolicyFetcher.sourceId(source, shards);
  }

  /** R106: with no hinted hash, force a remote fetch; with one, trust what is already cloned. */
  public void refreshScope(Scopes.Scope scope, String hintedHash) {
    refreshScope(scope, hintedHash, false);
  }

  /**
   * R234: fetches, and publishes what changed to the scope's own clients.
   *
   * <p>The head before and after are read around the fetch, because the difference between them
   * is the whole of what a client needs to be told. A scope seen for the first time is told about
   * every directory it holds rather than about a difference, since there is nothing to difference
   * against.
   */
  public void refreshScope(Scopes.Scope scope, String hintedHash, boolean honorBackoff) {
    GitPolicyFetcher fetcher = fetcherFor(scope).withHintedHash(hintedHash);
    ObjectId before = headOf(fetcher);
    fetcher.fetch(hintedHash == null, honorBackoff);
    ObjectId after = headOf(fetcher);
    notifyOnChange(scope, fetcher, before, after);
  }

  private ObjectId headOf(GitPolicyFetcher fetcher) {
    try {
      return fetcher.head();
    } catch (RuntimeException e) {
      return null;
    }
  }

  /** R234: what a scope's clients are told after a sync that moved the head. */
  private void notifyOnChange(
      Scopes.Scope scope, GitPolicyFetcher fetcher, ObjectId before, ObjectId after) {
    if (after == null || after.equals(before)) {
      if (after != null) {
        log.debug("scope '{}': No new commits, HEAD is at '{}'", scope.scope_id(), after.getName());
      }
      return;
    }
    log.info(
        "scope '{}': Found new commits: old HEAD was '{}', new HEAD is '{}'",
        scope.scope_id(),
        before == null ? "none" : before.getName(),
        after.getName());
    try {
      Repository repository = fetcher.repository();
      if (repository == null) {
        return;
      }
      Policy.PolicyUpdateMessageNotification notification =
          PolicyUpdates.createPolicyUpdate(
              repository,
              before == null ? after : before,
              after,
              scope.policy().extensions(),
              scope.policy().bundle_ignore());
      if (notification == null) {
        return;
      }
      // R235: every topic carries the scope's name in front, so only that tenant's clients hear
      // it. Without the prefix a hundred tenants on one server all refetch on any one's commit.
      List<String> topics = new ArrayList<>();
      for (String topic : notification.topics()) {
        topics.add(scope.scope_id() + ":" + topic);
      }
      log.info("Triggering policy update for scope {}", scope.scope_id());
      publish.accept(topics, notification.update());
    } catch (Exception e) {
      log.error("could not publish a policy update for scope {}: {}", scope.scope_id(), e.toString());
    }
  }

  public void syncAllScopes() {
    syncAllScopes(false, false);
  }

  /**
   * R236 and R237: one pass over every scope, each distinct repository fetched once.
   *
   * <p>Two phases. The first fetches each distinct source, so a hundred scopes sharing one
   * repository cost one network round trip rather than a hundred; the second checks the rest
   * against what is now on disk. Both are bounded, because the alternative is a thousand
   * simultaneous connections to somebody else's git host.
   *
   * @param onlyPollUpdates skip scopes whose source says not to poll it, which is what the
   *     periodic pass does and an explicit refresh does not
   * @param honorBackoff skip sources that have been failing, which only a periodic pass does
   */
  public void syncAllScopes(boolean onlyPollUpdates, boolean honorBackoff) {
    List<Scopes.Scope> all = scopes.all();
    metrics.gauge("opal_server.scopes.count", (long) all.size(), null);
    GitOps.emitSourcesInBackoff();

    List<Scopes.Scope> wanted = new ArrayList<>();
    for (Scopes.Scope scope : all) {
      if (onlyPollUpdates && !Boolean.TRUE.equals(scope.policy().poll_updates())) {
        continue;
      }
      wanted.add(scope);
    }

    Set<String> seenSources = new LinkedHashSet<>();
    List<Scopes.Scope> firstOfEachSource = new ArrayList<>();
    List<Scopes.Scope> rest = new ArrayList<>();
    for (Scopes.Scope scope : wanted) {
      if (seenSources.add(sourceIdOf(scope.policy()))) {
        firstOfEachSource.add(scope);
      } else {
        rest.add(scope);
      }
    }

    runBounded(firstOfEachSource, honorBackoff, true);
    runBounded(rest, honorBackoff, false);
  }

  /** R237: at most {@code maxConcurrentSyncs} scopes are synced at once. */
  private void runBounded(List<Scopes.Scope> batch, boolean honorBackoff, boolean fetchRemote) {
    if (batch.isEmpty()) {
      return;
    }
    ExecutorService pool =
        Executors.newFixedThreadPool(
            Math.min(maxConcurrentSyncs, batch.size()),
            runnable -> {
              Thread thread = new Thread(runnable, "opal-scope-sync");
              thread.setDaemon(true);
              return thread;
            });
    try {
      List<Future<?>> running = new ArrayList<>();
      for (Scopes.Scope scope : batch) {
        running.add(
            pool.submit(
                () -> {
                  try {
                    // A scope may have been deleted since the listing was taken; re-reading it
                    // is what keeps a pass from cloning a repository nobody wants any more.
                    Scopes.Scope current = scopes.find(scope.scope_id()).orElse(null);
                    if (current == null) {
                      return;
                    }
                    refreshScope(current, fetchRemote ? null : "", honorBackoff);
                  } catch (RuntimeException e) {
                    log.warn("could not sync scope {}: {}", scope.scope_id(), e.toString());
                  }
                }));
      }
      for (Future<?> future : running) {
        try {
          future.get();
        } catch (Exception e) {
          log.warn("a scope sync failed: {}", e.toString());
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * R238: every scope's repository is cloned at start-up, before anything asks for a bundle.
   *
   * <p>Without it the first request for each scope pays for the clone, which is the request most
   * likely to be a client that has just started and has no policy at all.
   */
  public void preloadScopes(double drainTimeoutSeconds) {
    List<Scopes.Scope> all = scopes.all();
    if (all.isEmpty()) {
      return;
    }
    log.info("Preloading {} scope(s)", all.size());
    syncAllScopes(false, true);
    double waited = 0;
    while (GitOps.busyCount() > 0 && waited < drainTimeoutSeconds) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      waited += 0.1;
    }
    if (GitOps.busyCount() > 0) {
      log.warn(
          "{} git operation(s) were still running when the preload gave up waiting",
          GitOps.busyCount());
    }
  }

  /** Announces that a source's clone may now be unused; every replica acts on it. */
  public void publishPurge(String sourceId, String scopeId) {
    publish.accept(List.of(purgeChannel), new ScopePurgeCommand(sourceId, scopeId));
  }

  /** R111 and R239: the clone goes only when nothing else still points at that source. */
  public void applyPurge(JsonNode command) {
    ScopePurgeCommand purge;
    try {
      purge = Rpc.MAPPER.treeToValue(command, ScopePurgeCommand.class);
    } catch (Exception e) {
      log.warn("could not read a scope purge command: {}", command);
      return;
    }
    if (purge.source_id() == null) {
      return;
    }
    Path clonePath = confinedClonePath(purge.source_id());
    if (clonePath == null) {
      log.warn(
          "refusing to purge a clone path outside the scopes directory: {}", purge.source_id());
      return;
    }
    // R267: the sibling check is bounded. A store that will not answer would otherwise hold the
    // source's entry for the life of the process and block every later sync of it; on expiry the
    // check fails open, which keeps a clone a live sibling may still share.
    List<String> survivingSourceIds;
    try {
      survivingSourceIds = siblingSourceIds();
    } catch (RuntimeException e) {
      log.warn(
          "could not read the scopes within {}s; keeping the clone for source {}",
          storeReadTimeoutSeconds,
          purge.source_id());
      GitPolicyFetcher.forgetRepo(clonePath.toString());
      return;
    }
    GitPolicyFetcher.forgetRepo(clonePath.toString());
    if (survivingSourceIds.contains(purge.source_id())) {
      log.info("source {} is still named by another scope; keeping its clone", purge.source_id());
      return;
    }
    try {
      RepoCloner.deleteRecursively(clonePath);
      log.info("removed the clone for source {}", purge.source_id());
    } catch (Exception e) {
      log.warn("could not remove the clone for source {}: {}", purge.source_id(), e.toString());
    }
  }

  /**
   * R267: every surviving scope's source id, read within the configured timeout.
   *
   * <p>Zero or negative means no timeout, which is what the entry's own description says.
   */
  private List<String> siblingSourceIds() {
    java.util.concurrent.ExecutorService reader =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-scope-sibling-read");
              thread.setDaemon(true);
              return thread;
            });
    try {
      Future<List<String>> reading =
          reader.submit(
              () -> {
                List<String> ids = new ArrayList<>();
                for (Scopes.Scope scope : scopes.all()) {
                  ids.add(sourceIdOf(scope.policy()));
                }
                return ids;
              });
      if (storeReadTimeoutSeconds <= 0) {
        return reading.get();
      }
      return reading.get((long) (storeReadTimeoutSeconds * 1000), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted reading the scopes", e);
    } catch (Exception e) {
      throw new IllegalStateException("could not read the scopes", e);
    } finally {
      reader.shutdownNow();
    }
  }

  /** R267: how long a purge waits for the store before deciding without it. */
  private volatile double storeReadTimeoutSeconds = 10;

  public void setStoreReadTimeout(double seconds) {
    this.storeReadTimeoutSeconds = seconds;
  }

  /**
   * R239: the path a source id resolves to, or nothing when it resolves outside.
   *
   * <p>The id arrives over the backbone, so it is not this replica's own value; a value with a
   * path separator in it would otherwise name any directory the process can reach.
   */
  Path confinedClonePath(String sourceId) {
    Path root = GitPolicyFetcher.baseDir(baseDir).toAbsolutePath().normalize();
    Path resolved = root.resolve(sourceId).toAbsolutePath().normalize();
    return resolved.startsWith(root) && !resolved.equals(root) ? resolved : null;
  }

  /**
   * R240: deletes the record, then publishes the purge for the source it was using.
   *
   * <p>The purge is published whether or not the record was there to read. A delete whose read
   * failed has still removed the record, and a replica left holding a cache entry for a scope
   * that no longer exists serves a bundle nobody can change.
   */
  public void deleteScope(String scopeId) {
    Scopes.Scope existing = null;
    try {
      existing = scopes.find(scopeId).orElse(null);
    } catch (RuntimeException e) {
      log.warn("could not read scope {} before deleting it: {}", scopeId, e.toString());
    }
    try {
      scopes.delete(scopeId);
    } finally {
      if (existing != null) {
        publishPurge(sourceIdOf(existing.policy()), scopeId);
      }
    }
  }

  /** R240: waits for the purges a delete started, so a shutdown does not cut one in half. */
  public void stop(double drainTimeoutSeconds) {
    for (Future<?> purge : localPurges) {
      try {
        purge.get((long) (drainTimeoutSeconds * 1000), TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        log.warn("a clone purge was still running at shutdown: {}", e.toString());
      }
    }
    localPurges.clear();
  }

  /** Whether the clone directory for a source is there at all, which the wait route reads. */
  public boolean cloneExists(Scopes.Scope scope) {
    Path path = GitPolicyFetcher.baseDir(baseDir).resolve(sourceIdOf(scope.policy()));
    return Files.isDirectory(path.resolve(".git"));
  }

  /** Whether a bundle could be built from the clone right now. */
  public boolean cloneReady(Scopes.Scope scope) {
    try (Git ignored = null) {
      return fetcherFor(scope).head() != null;
    } catch (RuntimeException e) {
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  Map<String, Object> stateForTests() {
    return Map.of("purges", localPurges.size());
  }
}
