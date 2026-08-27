package io.akka.opal.server.scopes;

import io.akka.opal.common.git.BundleMaker;
import io.akka.opal.common.git.RepoCloner;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.PolicySource;
import io.akka.opal.common.util.Hashing;
import io.akka.opal.common.util.Urls;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One scope's clone, and the bundle built from it — SPEC-002 R107 and R111.
 *
 * <p>The clone is named after the source rather than after the scope, so two scopes pointing at
 * the same repository and branch share one clone on disk. That is what makes a hundred tenants on
 * one policy repository cost one clone rather than a hundred, and it is why a purge has to check
 * whether any surviving scope still names the source before removing it.
 */
public final class GitPolicyFetcher {

  private static final Logger log = LoggerFactory.getLogger(GitPolicyFetcher.class);

  /** Open repositories, keyed by clone path — the caches the internal stats route reports. */
  private static final Map<String, Git> REPOS = new ConcurrentHashMap<>();
  private static final Map<String, Object> REPO_LOCKS = new ConcurrentHashMap<>();
  private static final Map<String, Long> REPOS_LAST_FETCHED = new ConcurrentHashMap<>();

  private final Path baseDir;
  private final String scopeId;
  private final PolicySource.GitPolicyScopeSource source;
  private final String sourceId;
  private final List<String> policyExtensions;

  public GitPolicyFetcher(
      Path baseDir,
      String scopeId,
      PolicySource.GitPolicyScopeSource source,
      int shards,
      List<String> policyExtensions) {
    this.baseDir = baseDir(baseDir);
    this.scopeId = scopeId;
    this.source = source;
    this.sourceId = sourceId(source, shards);
    this.policyExtensions = policyExtensions;
  }

  /**
   * R111 and R227: the clone's name — the url's digest, sharded by a byte of the branch's digest.
   *
   * <p>The byte is read as a number between 0 and 255. Read as a signed value it is negative half
   * the time, and two servers that disagree about which shard a scope belongs to clone the same
   * repository into two directories and each believes the other's is stale.
   */
  public static String sourceId(PolicySource.GitPolicyScopeSource source, int shards) {
    String base = Hashing.sha256(source.url());
    byte[] branchDigest = digest(source.branch());
    int index = (branchDigest[0] & 0xff) % Math.max(1, shards);
    return base + "-" + index;
  }

  private static byte[] digest(String text) {
    try {
      return java.security.MessageDigest.getInstance("SHA-256")
          .digest(text.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static Path baseDir(Path base) {
    return base.resolve("git_sources");
  }

  public static Path repoClonePath(Path base, PolicySource.GitPolicyScopeSource source, int shards) {
    return baseDir(base).resolve(sourceId(source, shards));
  }

  public String sourceId() {
    return sourceId;
  }

  public Path clonePath() {
    return baseDir.resolve(sourceId);
  }

  /**
   * R228: clones when the path holds nothing, and pulls when it does — bounded, and remembered.
   *
   * <p>Every attempt goes through the shared limits: one at a time per source, a ceiling on how
   * many run at once, and a timeout that abandons the wait rather than the pass. A source that
   * fails is recorded, so the next periodic pass skips it for a while; a source that answers
   * clears whatever it had recorded.
   *
   * @param honorBackoff false for an attempt somebody asked for by hand, which is never skipped —
   *     an operator who has just repaired a credential should not wait out a delay armed before
   *     the repair
   */
  public void fetch(boolean forceFetch, boolean honorBackoff) {
    this.honorBackoffUnderLock = honorBackoff;
    if (honorBackoff && GitOps.inBackoff(sourceId)) {
      GitOps.skipped("backoff");
      log.debug(
          "Skipping sync for {}: in backoff for another {}s after {} consecutive failures",
          Urls.redactUrl(source.url()),
          Math.round(GitOps.secondsUntilRetry(sourceId)),
          GitOps.consecutiveFailures(sourceId));
      return;
    }
    if (GitOps.inFlight(sourceId)) {
      GitOps.skipped("in_flight");
      log.debug("Skipping sync for {}: an operation is already running", sourceId);
      return;
    }
    try {
      GitOps.run(sourceId, () -> {
        doFetch(forceFetch);
        return null;
      });
      GitOps.clearFailure(sourceId);
    } catch (GitOps.OperationTimedOut e) {
      GitOps.failed(cloneExists() ? "fetch" : "clone", "timeout");
      GitOps.recordFailure(sourceId, Urls.redactUrl(source.url()), e.getMessage());
      log.error("Timed out fetching {}, skipping: {}", Urls.redactUrl(source.url()), e.getMessage());
    } catch (GitOps.ConcurrencyLimitExceeded e) {
      // Expected backpressure rather than a fault: the pass moves on and tries again next time.
      log.warn("Refusing a git operation for {}: {}", sourceId, e.getMessage());
    } catch (RuntimeException e) {
      GitOps.failed(cloneExists() ? "fetch" : "clone", "git_error");
      GitOps.recordFailure(
          sourceId,
          Urls.redactUrl(source.url()),
          Urls.redactUrlInText(String.valueOf(e.getMessage()), source.url()));
      throw e;
    }
  }

  /** The shape the rest of the rebuild calls, which honours the backoff. */
  public void fetch(boolean forceFetch) {
    fetch(forceFetch, true);
  }

  /** R396: what the pre-lock check decided, so the second look under the lock matches it. */
  private volatile boolean honorBackoffUnderLock;


  private boolean cloneExists() {
    return Files.isDirectory(clonePath().resolve(".git"));
  }

  /**
   * R395: asked, under the lock, whether this scope still wants this source.
   *
   * <p>Answering false means the scope was deleted or repointed while this attempt was queued,
   * and cloning then leaves a directory nothing later names.
   */
  private volatile java.util.function.BooleanSupplier livenessProbe;

  public void setLivenessProbe(java.util.function.BooleanSupplier probe) {
    this.livenessProbe = probe;
  }

  private void doFetch(boolean forceFetch) {
    Object lock = REPO_LOCKS.computeIfAbsent(clonePath().toString(), ignored -> new Object());
    synchronized (lock) {
      // R396: the backoff is looked at again here. Several syncs of one source can all pass the
      // check before the first of them has failed and recorded, and without this second look
      // each performs its own full attempt against a remote that is already known to be down.
      if (honorBackoffUnderLock && GitOps.inBackoff(sourceId)) {
        GitOps.skipped("backoff");
        log.debug(
            "Skipping sync for {}: in backoff for another {}s after {} consecutive failures",
            Urls.redactUrl(source.url()),
            Math.round(GitOps.secondsUntilRetry(sourceId)),
            GitOps.consecutiveFailures(sourceId));
        return;
      }
      java.util.function.BooleanSupplier probe = livenessProbe;
      if (probe != null && !probe.getAsBoolean()) {
        log.info("Skipping sync for {}: the scope no longer names this source", sourceId);
        return;
      }
      Path path = clonePath();
      try {
        Files.createDirectories(baseDir);
        if (!validClone(path)) {
          // R229: a directory that is not a usable clone is removed rather than opened. A clone
          // whose object store was truncated has intact refs and no objects, and every bundle
          // built from it fails in a way that reads like a policy error.
          forgetRepo(path.toString(), sourceId());
          if (Files.exists(path)) {
            RepoCloner.deleteRecursively(path);
          }
          RepoCloner cloner = cloner(path);
          Git git = cloner.cloneOrOpen();
          REPOS.put(path.toString(), git);
          REPOS_LAST_FETCHED.put(path.toString(), System.currentTimeMillis());
          return;
        }
        Git git = REPOS.computeIfAbsent(path.toString(), ignored -> open(path));
        if (shouldFetch(forceFetch, path, git)) {
          var command = git.fetch().setRemote("origin");
          RepoCloner cloner = cloner(path);
          cloner.applyTransport(command::setTransportConfigCallback);
          var credentials = credentials();
          if (credentials != null) {
            command.setCredentialsProvider(credentials);
          }
          command.call();
          REPOS_LAST_FETCHED.put(path.toString(), System.currentTimeMillis());
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(
            "could not fetch scope " + scopeId + " from "
                + Urls.redactUrl(source.url()), e);
      }
    }
  }

  private RepoCloner cloner(Path path) {
    RepoCloner cloner =
        new RepoCloner(
            source.url(),
            path.toString(),
            source.branch(),
            sshKey(),
            (int) GitOps.settings().fetchTimeoutSeconds());
    cloner.setCredentials(credentials());
    return cloner;
  }

  /**
   * R230: whether a clone that exists is one this can use.
   *
   * <p>A directory with a {@code .git} in it is not enough: the remote it points at may be a
   * different repository entirely, because the clone is named after a digest of the URL and a
   * scope can be repointed under a name that survives.
   */
  private boolean validClone(Path path) {
    // R314: a `.git` file — a worktree or submodule pointer — is a clone too. Requiring a
    // directory classifies one as "not a clone" and removes the whole tree.
    if (!Files.exists(path.resolve(".git"))) {
      return false;
    }
    try {
      Git git = REPOS.computeIfAbsent(path.toString(), ignored -> open(path));
      String remote = git.getRepository().getConfig().getString("remote", "origin", "url");
      if (remote != null && !remote.equals(source.url())) {
        // R315: the clone belongs to a different repository. The source raises rather than
        // recovering, which leaves the directory alone — deleting it here would destroy a clone
        // that a sibling scope, or a configuration typo about to be corrected, still needs.
        throw new IllegalStateException(
            "clone at " + path + " points at " + Urls.redactUrl(remote)
                + ", not at " + Urls.redactUrl(source.url()));
      }
      // R316: refs without objects. A clone whose object store was truncated resolves its head
      // and fails to read it, and every bundle built from it fails in a way that reads like a
      // policy error. Probed with a handle of its own, because the cached one keeps deleted
      // packs readable through its own memory mappings.
      return objectStoreHoldsItsHead(path);
    } catch (IllegalStateException e) {
      throw e;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private boolean objectStoreHoldsItsHead(Path path) {
    try (Git probe = Git.open(path.toFile())) {
      Ref head = probe.getRepository().findRef("refs/remotes/origin/" + source.branch());
      if (head == null || head.getObjectId() == null) {
        return true;
      }
      return probe.getRepository().getObjectDatabase().has(head.getObjectId());
    } catch (Exception e) {
      log.warn("Invalid repo at: {}", path);
      return false;
    }
  }

  /**
   * R231: whether this attempt has to reach the remote at all.
   *
   * <p>An explicit refresh does. A hint naming a commit the clone already holds does not — which
   * is the whole point of the hint: a webhook naming the commit a client is asking about saves
   * every scope on that repository a network round trip. And a clone whose configured branch is
   * missing does, whatever else is true, because there is nothing to build a bundle from.
   */
  private boolean shouldFetch(boolean forceFetch, Path path, Git git) {
    if (forceFetch) {
      // R312: a forced fetch is dropped when the clone was already fetched after the request was
      // made. A burst of refreshes for one scope otherwise reaches the remote once per request.
      Long lastFetched = REPOS_LAST_FETCHED.get(path.toString());
      if (requestedAtMillis > 0 && lastFetched != null && lastFetched >= requestedAtMillis) {
        log.info("Repo was fetched after refresh request, override force_fetch with False");
      } else {
        return true;
      }
    }
    // R313: the missing-branch check comes first. A hint naming a commit the clone already holds
    // would otherwise suppress the fetch of a branch that has never been fetched at all, and the
    // bundle built afterwards has no head to build from.
    try {
      if (git.getRepository().resolve("refs/remotes/origin/" + source.branch()) == null) {
        log.info("Target branch was not found in local clone, re-fetching the remote");
        return true;
      }
    } catch (Exception e) {
      return true;
    }
    if (hintedHash != null && !hintedHash.isEmpty()) {
      try {
        if (git.getRepository().resolve(hintedHash) != null) {
          return false;
        }
      } catch (Exception e) {
        // A hash that will not even parse is one the clone does not hold.
      }
      log.info("Hinted commit hash was not found in local clone, re-fetching the remote");
      return true;
    }
    return false;
  }

  /**
   * When the request that asked for this fetch was made, or zero when nothing said.
   *
   * <p>R312 reads it: a fetch that already happened after this instant has done the work the
   * request wanted.
   */
  private long requestedAtMillis;

  public GitPolicyFetcher requestedAt(long millis) {
    this.requestedAtMillis = millis;
    return this;
  }

  /** A commit the caller already knows about, which makes a remote fetch unnecessary. */
  private String hintedHash;

  public GitPolicyFetcher withHintedHash(String hash) {
    this.hintedHash = hash;
    return this;
  }

  /** R232: the credentials a scope's own auth block carries, for a remote that needs them. */
  private org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider credentials() {
    if (source.auth() instanceof PolicySource.GitHubTokenAuthData token) {
      // R398: the username the source sends. Both names work against GitHub, and a self-hosted
      // remote checking the pair sees the same two values from either system only with this one.
      return RepoCloner.credentials("git", token.token());
    }
    if (source.auth() instanceof PolicySource.UserPassAuthData userpass) {
      return RepoCloner.credentials(userpass.username(), userpass.password());
    }
    return null;
  }

  private String sshKey() {
    if (source.auth() instanceof PolicySource.SSHAuthData ssh) {
      return ssh.private_key();
    }
    return null;
  }

  private static Git open(Path path) {
    try {
      return Git.open(path.toFile());
    } catch (Exception e) {
      throw new IllegalStateException("could not open " + path, e);
    }
  }

  /** Raised while the clone exists but holds no branch head yet. */
  public static final class CloneNotPopulated extends RuntimeException {
    public CloneNotPopulated(String message) {
      super(message);
    }
  }

  /** R233: raised when the clone holds branches and not the one the scope names. */
  public static final class BranchHeadNotFound extends RuntimeException {
    public BranchHeadNotFound(String message) {
      super(message);
    }
  }

  public Policy.PolicyBundle makeBundle(String baseHash) {
    Path path = clonePath();
    if (!Files.isDirectory(path.resolve(".git"))) {
      throw new CloneNotPopulated("clone for scope " + scopeId + " does not exist yet");
    }
    Git git = REPOS.computeIfAbsent(path.toString(), ignored -> open(path));
    Repository repository = git.getRepository();
    try {
      ObjectId head = resolveHead(git, repository);
      BundleMaker maker =
          new BundleMaker(
              repository,
              Set.copyOf(source.directories()),
              source.extensions(),
              source.manifest(),
              source.bundle_ignore(),
              policyExtensions);
      if (baseHash == null) {
        return maker.makeBundle(head);
      }
      // R307: a base hash the clone cannot resolve — absent, or not a revision at all — asks
      // for a difference against nothing, and the answer is the whole bundle. Letting the
      // library's own syntax error out would turn a caller's bad query parameter into a 503.
      ObjectId base;
      try {
        base = repository.resolve(baseHash);
      } catch (Exception e) {
        base = null;
      }
      if (base == null) {
        return maker.makeBundle(head);
      }
      return maker.makeDiffBundle(base, head);
    } catch (CloneNotPopulated | BranchHeadNotFound e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("could not build a bundle for scope " + scopeId, e);
    }
  }

  /** The commit the scope's branch is at, or nothing when the clone cannot answer yet. */
  public ObjectId head() {
    Path path = clonePath();
    if (!Files.isDirectory(path.resolve(".git"))) {
      return null;
    }
    try {
      Git git = REPOS.computeIfAbsent(path.toString(), ignored -> open(path));
      return resolveHead(git, git.getRepository());
    } catch (CloneNotPopulated | BranchHeadNotFound e) {
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /** The open repository behind the clone, for a caller that needs to read commits from it. */
  public Repository repository() {
    Path path = clonePath();
    if (!Files.isDirectory(path.resolve(".git"))) {
      return null;
    }
    return REPOS.computeIfAbsent(path.toString(), ignored -> open(path)).getRepository();
  }

  /**
   * The head of the branch this scope tracks.
   *
   * <p>Only the remote-tracking ref answers. R233 splits its absence by what is on disk rather
   * than by anything in flight: an empty {@code refs/remotes/origin/} namespace is a clone that
   * has not been populated yet and will answer soon, while siblings present and this one missing
   * is a branch that does not exist and never will. Falling back to another ref would serve one
   * branch's policy under another branch's name.
   */
  private ObjectId resolveHead(Git git, Repository repository) throws Exception {
    ObjectId head = repository.resolve("refs/remotes/origin/" + source.branch());
    if (head == null) {
      List<Ref> remotes = repository.getRefDatabase().getRefsByPrefix("refs/remotes/origin/");
      if (remotes.isEmpty()) {
        throw new CloneNotPopulated(
            "No refs/remotes/origin/* refs yet at " + clonePath());
      }
      log.error("Could not find current branch head");
      throw new BranchHeadNotFound("Could not find current branch head");
    }
    return head;
  }

  /**
   * R304: the ref this scope remembers its last-seen head on.
   *
   * <p>Scopes share a clone when they name the same repository and branch, so "did the head move"
   * cannot be answered by reading the clone before and after this scope's own fetch — another
   * scope's fetch may already have moved it. Each scope therefore keeps a local ref of its own
   * and compares against that. A scope id that is not a legal ref name is used as its own hex.
   */
  public String localBranchName() {
    String candidate = "scopes/" + scopeId;
    return Repository.isValidRefName("refs/heads/" + candidate)
        ? candidate
        : "scopes/" + io.akka.opal.common.util.Hashing.hex(scopeId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  /**
   * The head this scope last saw, and the head now, advancing the scope's own ref to the second.
   *
   * <p>The first is null the first time, which is what tells a caller to announce everything
   * rather than a difference.
   */
  public java.util.Map.Entry<ObjectId, ObjectId> advanceTrackedHead() {
    try {
      Path path = clonePath();
      if (!Files.isDirectory(path.resolve(".git"))) {
        return new java.util.AbstractMap.SimpleEntry<>(null, null);
      }
      Git git = REPOS.computeIfAbsent(path.toString(), ignored -> open(path));
      Repository repository = git.getRepository();
      ObjectId now = resolveHead(git, repository);
      Ref local = repository.findRef("refs/heads/" + localBranchName());
      ObjectId before = local == null ? null : local.getObjectId();
      org.eclipse.jgit.lib.RefUpdate update = repository.updateRef("refs/heads/" + localBranchName());
      update.setNewObjectId(now);
      update.setForceUpdate(true);
      update.update();
      return new java.util.AbstractMap.SimpleEntry<>(before, now);
    } catch (Exception e) {
      log.debug("could not read the tracked head for scope {}: {}", scopeId, e.toString());
      return new java.util.AbstractMap.SimpleEntry<>(null, null);
    }
  }

  /**
   * Drops the cached handle so a purge can remove the directory underneath it.
   *
   * <p>R306: the source's failure history is keyed by its id, not by the path its clone happens
   * to sit at, so the id is what has to be forgotten. Passing the path removes nothing: the
   * gauge keeps counting a source nobody has, and a scope re-created against the same repository
   * inherits the deleted one's backoff and has its first sync skipped.
   */
  /**
   * The lock that guards one clone, so a purge and a sync of the same source cannot overlap.
   *
   * <p>Keyed by the clone path rather than the source id, which is the key the fetch path already
   * uses — two callers taking different keys for the same directory is the same as no lock.
   */
  public static Object lockForPath(String clonePath) {
    return REPO_LOCKS.computeIfAbsent(clonePath, ignored -> new Object());
  }

  /** Drops a source's failure history without touching the handle its clone path holds. */
  public static void forgetSourceBackoff(String sourceId) {
    GitOps.forgetSource(sourceId);
  }

  public static void forgetRepo(String path, String sourceId) {
    if (sourceId != null) {
      GitOps.forgetSource(sourceId);
    }
    Git git = REPOS.remove(path);
    if (git != null) {
      try {
        git.close();
      } catch (RuntimeException e) {
        // A handle that throws on close must not stop the directory being removed — the source
        // logs and carries on for the same reason.
        log.warn("could not close the repository handle at {}: {}", path, e.toString());
      }
    }
    REPO_LOCKS.remove(path);
    REPOS_LAST_FETCHED.remove(path);
  }

  /**
   * The process's resident set size in kilobytes, or zero where the platform will not say.
   *
   * <p>Read from {@code /proc/self/status} where there is one, the way the source reads it, and
   * from the platform's own accounting otherwise.
   */
  static long residentKilobytes() {
    Path status = Path.of("/proc/self/status");
    if (Files.isReadable(status)) {
      try {
        for (String line : Files.readAllLines(status)) {
          if (line.startsWith("VmRSS:")) {
            return Long.parseLong(line.replaceAll("[^0-9]", ""));
          }
        }
      } catch (Exception e) {
        return 0;
      }
      return 0;
    }
    try {
      com.sun.management.OperatingSystemMXBean os =
          (com.sun.management.OperatingSystemMXBean)
              java.lang.management.ManagementFactory.getOperatingSystemMXBean();
      long committed = os.getCommittedVirtualMemorySize();
      return committed <= 0 ? 0 : committed / 1024;
    } catch (Exception e) {
      return 0;
    }
  }

  /** R133: what the debug route reports about these caches. */
  public static Map<String, Object> cacheStats() {
    Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("pid", ProcessHandle.current().pid());
    stats.put("repos", REPOS.size());
    stats.put("repos_keys", REPOS.keySet().stream().sorted().toList());
    stats.put("repo_locks", REPO_LOCKS.size());
    stats.put("repo_locks_keys", REPO_LOCKS.keySet().stream().sorted().toList());
    stats.put("repos_last_fetched", REPOS_LAST_FETCHED.size());
    stats.put("repos_last_fetched_keys", REPOS_LAST_FETCHED.keySet().stream().sorted().toList());
    // R328: the process's resident set, not the heap. The field exists to watch the memory the
    // mapped pack files hold, and those are outside the heap entirely.
    stats.put("rss_kb", residentKilobytes());
    return stats;
  }
}
