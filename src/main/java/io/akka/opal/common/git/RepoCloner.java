package io.akka.opal.common.git;

import io.akka.opal.common.util.Urls;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.SshSessionFactory;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.transport.ssh.jsch.JschConfigSessionFactory;
import org.eclipse.jgit.transport.ssh.jsch.OpenSshConfig;
import org.eclipse.jgit.util.FS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clones the policy repository, or adopts one already on disk — SPEC-002 R37.
 *
 * <p>An existing clone is adopted only when its remote matches the configured URL. OPAL does not
 * remove and re-clone on a mismatch, and neither does this: a directory holding somebody else's
 * repository is a configuration mistake, and deleting it would destroy the evidence of that.
 */
public final class RepoCloner {

  private static final Logger log = LoggerFactory.getLogger(RepoCloner.class);

  /** Raised when git could not do what it was asked. */
  public static final class GitFailed extends RuntimeException {
    public GitFailed(String message) {
      super(message);
    }

    public GitFailed(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private final String url;
  private final Path path;
  private final String branchName;
  private final String sshKey;
  private final int cloneTimeoutSeconds;

  public RepoCloner(String url, String path, String branchName, String sshKey, int cloneTimeout) {
    // R283: refused where it is given rather than where it is used. A cloner with no url is a
    // misconfiguration, and the failure belongs at start-up where an operator sees it, not on
    // the first poll where it looks like a network fault.
    if (url == null || url.isEmpty()) {
      throw new IllegalArgumentException("must provide repo url!");
    }
    this.url = url;
    this.path = Path.of(expandUser(path));
    this.branchName = branchName;
    this.sshKey = sshKey;
    this.cloneTimeoutSeconds = cloneTimeout;
  }

  public Path path() {
    return path;
  }

  public String url() {
    return url;
  }

  /** Clones, or opens what is already there when its remote agrees with the configuration. */
  public Git cloneOrOpen() {
    if (Files.isDirectory(path.resolve(".git"))) {
      try {
        Git existing = Git.open(path.toFile());
        List<String> remotes =
            existing.remoteList().call().stream()
                .flatMap(remote -> remote.getURIs().stream())
                .map(URIish::toString)
                .toList();
        if (!remotes.contains(url)) {
          existing.close();
          throw new GitFailed("Existing repo has wrong remote url: " + remotes);
        }
        log.info(
            "SKIPPED cloning policy repo, found existing repo at '{}' with remotes: {}",
            path,
            remotes);
        return existing;
      } catch (GitFailed e) {
        throw e;
      } catch (Exception e) {
        throw new GitFailed("could not open existing repo at " + path, e);
      }
    }
    return doClone();
  }

  /**
   * R187: the clone is retried until it succeeds, or until the configured timeout has passed.
   *
   * <p>A policy repository that is briefly unreachable at start-up is the ordinary case this
   * exists for: without it a fleet coming up alongside its git host fails outright. The wait is
   * random and doubles, to a ceiling of thirty seconds, so a hundred clients do not arrive
   * together. With no timeout configured there is no stop condition at all, which is what the
   * source means by waiting indefinitely.
   */
  private Git doClone() {
    long deadline =
        cloneTimeoutSeconds > 0
            ? System.nanoTime() + cloneTimeoutSeconds * 1_000_000_000L
            : Long.MAX_VALUE;
    int attempt = 0;
    while (true) {
      attempt++;
      try {
        return attemptClone();
      } catch (GitFailed e) {
        if (System.nanoTime() >= deadline) {
          throw e;
        }
        double wait = Math.min(CLONE_MAX_WAIT_SECONDS, 0.5 * Math.pow(2, attempt));
        long millis = (long) (java.util.concurrent.ThreadLocalRandom.current()
            .nextDouble(0, Math.max(wait, 0.001)) * 1000);
        if (System.nanoTime() + millis * 1_000_000L >= deadline) {
          throw e;
        }
        log.warn(
            "could not clone {}, retrying in {} ms: {}",
            Urls.redactUrl(url),
            millis,
            Urls.redactUrlInText(String.valueOf(e.getMessage()), url));
        try {
          Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  /** The ceiling on the wait between clone attempts. */
  static final double CLONE_MAX_WAIT_SECONDS = 30;

  private Git attemptClone() {
    try {
      Files.createDirectories(path.getParent() == null ? path : path.getParent());
      var command =
          Git.cloneRepository()
              .setURI(url)
              .setDirectory(path.toFile())
              .setBranch(branchName)
              .setCloneAllBranches(true);
      if (cloneTimeoutSeconds > 0) {
        command.setTimeout(cloneTimeoutSeconds);
      }
      applyTransport(command::setTransportConfigCallback);
      if (credentials != null) {
        command.setCredentialsProvider(credentials);
      }
      return command.call();
    } catch (Exception e) {
      throw new GitFailed("could not clone " + Urls.redactUrl(url) + " into " + path, e);
    }
  }

  /**
   * The SSH key OPAL is given is a key rather than a path, and jgit's default session factory
   * reads {@code ~/.ssh}. Writing it to a file the factory is pointed at is the one way to make
   * jgit use it without changing the process's own SSH configuration.
   */
  public void applyTransport(java.util.function.Consumer<TransportConfigCallback> setter) {
    // R273: the key is for ssh, and a url that is not an ssh url never sees it — the source
    // returns an empty environment before it writes anything to disk.
    if (sshKey == null || sshKey.isEmpty() || !isSshRepoUrl(url)) {
      return;
    }
    Path keyFile = writeKeyFile();
    SshSessionFactory factory =
        new JschConfigSessionFactory() {
          @Override
          protected void configure(OpenSshConfig.Host host, com.jcraft.jsch.Session session) {
            session.setConfig("StrictHostKeyChecking", "no");
            // R274: IdentitiesOnly, so the agent's other keys are not offered first to a host
            // that counts failed attempts.
            session.setConfig("IdentitiesOnly", "yes");
          }

          @Override
          protected com.jcraft.jsch.JSch createDefaultJSch(FS fs) throws com.jcraft.jsch.JSchException {
            com.jcraft.jsch.JSch jsch = new com.jcraft.jsch.JSch();
            jsch.addIdentity(keyFile.toString());
            return jsch;
          }
        };
    setter.accept(
        transport -> {
          if (transport instanceof SshTransport ssh) {
            ssh.setSshSessionFactory(factory);
          }
        });
  }

  /** An ssh url is {@code ssh://...} or {@code git@...}, which is the source's own test. */
  public static boolean isSshRepoUrl(String repoUrl) {
    return repoUrl != null && (repoUrl.startsWith("ssh://") || repoUrl.startsWith("git@"));
  }

  /**
   * R186: the key is written where {@code GIT_SSH_KEY_FILE} says, readable only by its owner.
   *
   * <p>Underscores in the configured value become newlines. A private key spans many lines and an
   * environment variable is one, so the source accepts the whole key on a single line with that
   * substitution — a key already carrying real newlines is unaffected, because it has no
   * underscores to replace.
   */
  private Path writeKeyFile() {
    try {
      Path keyFile = Path.of(expandUser(sshKeyFile));
      Path parent = keyFile.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      String key = sshKey.replace("_", "\n");
      Files.writeString(keyFile, key.endsWith("\n") ? key : key + "\n");
      try {
        Files.setPosixFilePermissions(
            keyFile, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
      } catch (UnsupportedOperationException ignored) {
        // Windows has no POSIX permissions; the file sits under the running user's own home.
      }
      return keyFile;
    } catch (IOException e) {
      throw new GitFailed("could not write the ssh key to disk", e);
    }
  }

  /** R185: the two shapes of URL that mean SSH. */
  public static boolean isSshUrl(String repoUrl) {
    return repoUrl != null && (repoUrl.startsWith("ssh://") || repoUrl.startsWith("git@"));
  }

  /**
   * Where a key is written, read once at start-up from {@code GIT_SSH_KEY_FILE}.
   *
   * <p>Held statically because the source reads the same global entry at each of its own call
   * sites, and because a cloner is built in several places that do not all carry configuration.
   */
  private static volatile String sshKeyFile =
      System.getProperty("user.home") + "/.ssh/opal_repo_ssh_key";

  public static void configureSshKeyFile(String path) {
    if (path != null && !path.isBlank()) {
      sshKeyFile = path;
    }
  }

  /** Credentials for an HTTPS remote that needs a token or a username and password. */
  public static UsernamePasswordCredentialsProvider credentials(String username, String password) {
    return new UsernamePasswordCredentialsProvider(username, password);
  }

  /**
   * R232: the credentials this cloner presents, where the remote needs any.
   *
   * <p>Set separately from the constructor because the two callers that have them — a scope whose
   * auth block names a token or a password — build the cloner through a shared path that does not
   * carry either.
   */
  public void setCredentials(UsernamePasswordCredentialsProvider provider) {
    this.credentials = provider;
  }

  private UsernamePasswordCredentialsProvider credentials;

  public static String expandUser(String value) {
    if (value.startsWith("~")) {
      return System.getProperty("user.home") + value.substring(1);
    }
    return value;
  }

  public static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (var walk = Files.walk(root)) {
      walk.sorted(java.util.Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  public static Repository open(Path path) throws IOException {
    return Git.open(path.toFile()).getRepository();
  }

  public static Transport unusedTransportHandle() {
    throw new UnsupportedOperationException();
  }
}
