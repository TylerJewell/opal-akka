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

  private boolean cloneExists() {
    return Files.isDirectory(clonePath().resolve(".git"));
  }

  private void doFetch(boolean forceFetch) {
    Object lock = REPO_LOCKS.computeIfAbsent(clonePath().toString(), ignored -> new Object());
    synchronized (lock) {
      Path path = clonePath();
      try {
        Files.createDirectories(baseDir);
        if (!validClone(path)) {
          // R229: a directory that is not a usable clone is removed rather than opened. A clone
          // whose object store was truncated has intact refs and no objects, and every bundle
          // built from it fails in a way that reads like a policy error.
          forgetRepo(path.toString());
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
    if (!Files.isDirectory(path.resolve(".git"))) {
      return false;
    }
    try {
      Git git = REPOS.computeIfAbsent(path.toString(), ignored -> open(path));
      String remote = git.getRepository().getConfig().getString("remote", "origin", "url");
      return remote == null || remote.equals(source.url());
    } catch (RuntimeException e) {
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
      return true;
    }
    if (hintedHash != null && !hintedHash.isEmpty()) {
      try {
        return git.getRepository().resolve(hintedHash) == null;
      } catch (Exception e) {
        return true;
      }
    }
    try {
      return git.getRepository().resolve("refs/remotes/origin/" + source.branch()) == null;
    } catch (Exception e) {
      return true;
    }
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
      return RepoCloner.credentials("x-access-token", token.token());
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
      ObjectId base = repository.resolve(baseHash);
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

  private ObjectId resolveHead(Git git, Repository repository) throws Exception {
    ObjectId head = repository.resolve("refs/remotes/origin/" + source.branch());
    if (head == null) {
      head = repository.resolve(source.branch());
    }
    if (head == null) {
      head = repository.resolve("HEAD");
    }
    if (head == null) {
      List<String> refs = new ArrayList<>();
      for (Ref ref : git.branchList().call()) {
        refs.add(ref.getName());
      }
      // R233: a clone with no refs at all is still being populated and will answer soon; one
      // with other branches and not this one is a configuration that will never resolve, and a
      // caller told to retry would retry forever.
      if (refs.isEmpty()) {
        throw new CloneNotPopulated(
            "no branch head for " + source.branch() + " in " + clonePath() + " yet");
      }
      throw new BranchHeadNotFound(
          "no branch head for " + source.branch() + " in " + clonePath() + ", found " + refs);
    }
    return head;
  }

  /** Drops the cached handle so a purge can remove the directory underneath it. */
  public static void forgetRepo(String path) {
    GitOps.forgetSource(path);
    Git git = REPOS.remove(path);
    if (git != null) {
      git.close();
    }
    REPO_LOCKS.remove(path);
    REPOS_LAST_FETCHED.remove(path);
  }

  /** R133: what the debug route reports about these caches. */
  public static Map<String, Object> cacheStats() {
    Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("pid", ProcessHandle.current().pid());
    stats.put("repos", REPOS.size());
    stats.put("repos_keys", new ArrayList<>(REPOS.keySet()));
    stats.put("repo_locks", REPO_LOCKS.size());
    stats.put("repo_locks_keys", new ArrayList<>(REPO_LOCKS.keySet()));
    stats.put("repos_last_fetched", REPOS_LAST_FETCHED.size());
    stats.put("repos_last_fetched_keys", new ArrayList<>(REPOS_LAST_FETCHED.keySet()));
    stats.put("rss_kb", Runtime.getRuntime().totalMemory() / 1024);
    return stats;
  }
}
