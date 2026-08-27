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

  /**
   * What a purge names: the source whose clone may go, the scope that triggered it, and why.
   *
   * <p>R389 — the reason is load-bearing rather than commentary. A repoint's old source still has
   * a live record somewhere, so a sibling check that could not be performed must not be read as
   * "nobody else uses it"; a delete's record is already gone, so the same failed check purges
   * defensively, because under-purging there leaks the clone for good.
   */
  public record ScopePurgeCommand(String source_id, String scope_id, String reason) {

    public ScopePurgeCommand {
      if (reason == null || reason.isEmpty()) {
        reason = "delete";
      }
    }
  }

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
    GitPolicyFetcher fetcher =
        new GitPolicyFetcher(baseDir, scope.scope_id(), scope.policy(), shards, policyExtensions);
    // R395: asked again immediately before the clone begins, not only before the queue was
    // joined. A scope deleted or repointed while this was waiting for a worker would otherwise
    // be cloned into a directory the purge that followed the delete has already been past, and
    // nothing later names that source to reclaim it.
    String sourceId = sourceIdOf(scope.policy());
    fetcher.setLivenessProbe(
        () -> {
          try {
            Scopes.Scope fresh = scopes.find(scope.scope_id()).orElse(null);
            return fresh != null && sourceId.equals(sourceIdOf(fresh.policy()));
          } catch (RuntimeException e) {
            log.warn(
                "could not re-read scope {} before cloning it: {}",
                scope.scope_id(),
                e.toString());
            return true;
          }
        });
    return fetcher;
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
    refreshScope(scope, hintedHash, honorBackoff, true);
  }

  /**
   * R304: fetches, and — unless told not to — publishes what changed to the scope's own clients.
   *
   * <p>The heads are read from the scope's own tracking ref rather than from the clone before and
   * after this fetch: two scopes on the same repository share one clone, so the second scope's
   * "before" would already carry the first scope's fetch and it would announce nothing.
   */
  public void refreshScope(
      Scopes.Scope scope, String hintedHash, boolean honorBackoff, boolean notifyOnChanges) {
    refreshScope(scope, hintedHash, honorBackoff, notifyOnChanges, System.currentTimeMillis());
  }

  /**
   * R312: the same, told when the request that asked for it was made.
   *
   * <p>A burst of refreshes naming one scope arrives as several syncs of one clone, and the
   * second of them wants what the first already fetched. The instant is what lets the second
   * decide that without asking the remote again — every caller has one, and a sync given none
   * has always fetched.
   */
  public void refreshScope(
      Scopes.Scope scope,
      String hintedHash,
      boolean honorBackoff,
      boolean notifyOnChanges,
      long requestedAtMillis) {
    GitPolicyFetcher fetcher =
        fetcherFor(scope).withHintedHash(hintedHash).requestedAt(requestedAtMillis);
    fetcher.fetch(hintedHash == null, honorBackoff);
    if (!notifyOnChanges) {
      // Still advance the ref, so the first real sync announces a difference rather than
      // everything the repository holds.
      fetcher.advanceTrackedHead();
      return;
    }
    java.util.Map.Entry<ObjectId, ObjectId> moved = fetcher.advanceTrackedHead();
    notifyOnChange(scope, fetcher, moved.getKey(), moved.getValue());
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
              path -> PolicyUpdates.isRegoSourceFile(path, scope.policy().extensions()));
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

  /**
   * R287: writes the scope a single-tenant deployment's environment describes.
   *
   * <p>OPAL's own configuration names one policy repository, and a scoped server serves scopes —
   * so the leader turns that configuration into a scope called {@code default} before it polls.
   * Without it a scoped deployment configured the ordinary way has no scope at all, and the
   * fallback that serves the default scope's bundle to an unknown scope id can never fire.
   */
  public void loadScopes(Scopes.Scope fromConfiguration) {
    log.info("Server is primary, loading default scope.");
    if (fromConfiguration == null) {
      return;
    }
    log.info(
        "Adding default scope from env: {}",
        io.akka.opal.common.util.Urls.redactUrl(fromConfiguration.policy().url()));
    scopes.put(fromConfiguration);
  }

  /** One scope by id, for a trigger that names one. */
  public java.util.Optional<Scopes.Scope> findScope(String scopeId) {
    return scopes.find(scopeId);
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
    syncAllScopes(onlyPollUpdates, honorBackoff, true);
  }

  /**
   * The same, saying whether the pass announces what it finds.
   *
   * <p>The boot preload does not: every scope's first sync would otherwise announce every
   * directory it holds, so a server restart makes every client in the fleet refetch everything.
   */
  public void syncAllScopes(boolean onlyPollUpdates, boolean honorBackoff, boolean notifyOnChanges) {
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

    runBounded(firstOfEachSource, honorBackoff, true, notifyOnChanges);
    runBounded(rest, honorBackoff, false, notifyOnChanges);
  }

  /** R237: at most {@code maxConcurrentSyncs} scopes are synced at once. */
  private void runBounded(
      List<Scopes.Scope> batch, boolean honorBackoff, boolean fetchRemote, boolean notifyOnChanges) {
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
                    refreshScope(current, fetchRemote ? null : "", honorBackoff, notifyOnChanges);
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
    // R305: the preload announces nothing. Every scope's first sync would otherwise publish
    // every directory it holds, so a server restart wakes every client in the fleet at once.
    syncAllScopes(false, true, false);
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
    publishPurge(sourceId, scopeId, "delete");
  }

  public void publishPurge(String sourceId, String scopeId, String reason) {
    publish.accept(List.of(purgeChannel), new ScopePurgeCommand(sourceId, scopeId, reason));
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
    // R390: no new purge work once shutdown has started. A purge takes the source's lock and
    // then reads the store, and neither is worth beginning against a process that is going away.
    if (stopping) {
      log.info(
          "Ignoring purge request for {} ({}): the purger is stopping",
          purge.source_id(),
          purge.reason());
      return;
    }
    // R391: the id arrived over the backbone, so its shape is checked before it becomes a path.
    // A sha256 digest and a shard index have no separators, so anything else is not one of ours.
    if (!SOURCE_ID.matcher(purge.source_id()).matches()) {
      log.warn("Ignoring scope purge with malformed source_id: {}", purge.source_id());
      return;
    }
    Path clonePath = confinedClonePath(purge.source_id());
    if (clonePath == null) {
      log.warn(
          "refusing to purge a clone path outside the scopes directory: {}", purge.source_id());
      return;
    }
    // R392: the purge holds the source's own lock for the whole of it. A sync that is fetching
    // into this clone holds the same lock, and removing the tree underneath it is the one thing
    // no later sync repairs.
    Object lock = GitPolicyFetcher.lockForPath(clonePath.toString());
    synchronized (lock) {
      // R267: the sibling check is bounded. A store that will not answer would otherwise hold the
      // source's entry for the life of the process and block every later sync of it.
      List<String> survivingSourceIds;
      try {
        survivingSourceIds = siblingSourceIds();
      } catch (RuntimeException e) {
        // R389: which way the failure falls is decided by the reason. A repoint's old source may
        // still be named by a live scope, so an unanswered read keeps the clone; a delete's
        // record is already gone, so the same read purges rather than leaking the clone for good.
        if ("repoint".equals(purge.reason())) {
          log.warn(
              "Sibling check for {} timed out after {}s on a repoint; not confirming — the fleet"
                  + " keeps its cache entries for this source until something names it again",
              purge.source_id(),
              storeReadTimeoutSeconds);
          forgetLocally(clonePath, purge.source_id());
          return;
        }
        log.warn(
            "Sibling check for {} timed out after {}s on {}; confirming defensively — its record"
                + " is already gone, so withholding the purge would leak the fleet's cache"
                + " entries permanently",
            purge.source_id(),
            storeReadTimeoutSeconds,
            purge.reason());
        survivingSourceIds = List.of();
      }
      forgetLocally(clonePath, purge.source_id());
      if (survivingSourceIds.contains(purge.source_id())) {
        log.info("source {} is still named by another scope; keeping its clone", purge.source_id());
        return;
      }
      // R393: a git operation still running against this clone keeps it. The lock above covers
      // this process's own syncs; a fetch that outlived its timeout is on a thread that never
      // took it, and removing the tree under that thread is what the marker exists to prevent.
      if (GitOps.inFlight(purge.source_id())) {
        log.info(
            "Skipping the local clone purge for {}: a git operation is still in flight",
            purge.source_id());
        return;
      }
      try {
        RepoCloner.deleteRecursively(clonePath);
        log.info("removed the clone for source {}", purge.source_id());
      } catch (Exception e) {
        log.warn("could not remove the clone for source {}: {}", purge.source_id(), e.toString());
      }
    }
  }

  /**
   * Drops this process's own handle and failure history for a source.
   *
   * <p>Kept apart from the tree removal because they answer different questions: the cached
   * handle is this worker's, and the tree is shared with every worker on the same volume.
   */
  private void forgetLocally(Path clonePath, String sourceId) {
    if (GitOps.inFlight(sourceId)) {
      // Freeing a handle a thread is still reading is a crash rather than a leak.
      log.info("keeping the cached handle for {}: a git operation is still in flight", sourceId);
      GitPolicyFetcher.forgetSourceBackoff(sourceId);
      return;
    }
    GitPolicyFetcher.forgetRepo(clonePath.toString(), sourceId);
  }

  /** R391: a source id is a sha256 digest and a shard index, and nothing else. */
  private static final java.util.regex.Pattern SOURCE_ID =
      java.util.regex.Pattern.compile("[0-9a-f]{64}-[0-9]+");

  /** R390: set when shutdown begins, so no purge starts work the shutdown will abandon. */
  private volatile boolean stopping;

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
        String sourceId = sourceIdOf(existing.policy());
        publishPurge(sourceId, scopeId, "delete");
        // R394: this worker reclaims its own clone directly rather than waiting for its copy of
        // the message it just published. The purge runs off the request path — its first act is
        // to take the source's lock, which a sync can hold across a whole clone — and the future
        // is kept so a shutdown can wait for it rather than cutting it in half.
        localPurges.add(
            purgePool()
                .submit(
                    () ->
                        applyPurge(
                            Rpc.MAPPER.valueToTree(
                                new ScopePurgeCommand(sourceId, scopeId, "delete")))));
      }
    }
  }

  private ExecutorService purges;

  private synchronized ExecutorService purgePool() {
    if (purges == null) {
      purges =
          Executors.newCachedThreadPool(
              runnable -> {
                Thread thread = new Thread(runnable, "opal-scope-purge");
                thread.setDaemon(true);
                return thread;
              });
    }
    return purges;
  }

  /** R240: waits for the purges a delete started, so a shutdown does not cut one in half. */
  public void stop(double drainTimeoutSeconds) {
    stopping = true;
    for (Future<?> purge : localPurges) {
      try {
        purge.get((long) (drainTimeoutSeconds * 1000), TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        log.warn("a clone purge was still running at shutdown: {}", e.toString());
      }
    }
    localPurges.clear();
    if (purges != null) {
      purges.shutdownNow();
      purges = null;
    }
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
