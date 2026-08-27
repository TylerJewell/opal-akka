package io.akka.opal.common.git;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where the policy repository's clone lives — SPEC-002 R188–R190.
 *
 * <p>The clone goes in a subdirectory of the configured base, named after a prefix. By default the
 * name carries a random suffix, so a fresh run never inherits a half-written clone from the last
 * one; the directory is then found again by looking for the one subdirectory matching the prefix,
 * which is how a process that did not create it locates it. Where a deployment would rather keep
 * one directory across runs — a mounted volume, say — the prefix is the whole name and it is
 * reused.
 */
public final class ClonePathFinder {

  private static final Logger log = LoggerFactory.getLogger(ClonePathFinder.class);

  private final Path base;
  private final String prefix;
  private final boolean useFixedPath;

  public ClonePathFinder(String baseClonePath, String subdirectoryPrefix, boolean useFixedPath) {
    if (baseClonePath == null || baseClonePath.isEmpty()) {
      throw new IllegalArgumentException("base_clone_path cannot be empty!");
    }
    if (subdirectoryPrefix == null || subdirectoryPrefix.isEmpty()) {
      throw new IllegalArgumentException("clone_subdirectory_prefix cannot be empty!");
    }
    this.base = Path.of(RepoCloner.expandUser(baseClonePath));
    this.prefix = subdirectoryPrefix;
    this.useFixedPath = useFixedPath;
  }

  /**
   * R188: the clone directory if one is already there, and nothing if it is not.
   *
   * <p>With a random suffix this answers only when there is exactly one candidate: two of them
   * means an earlier run left one behind, and picking either would be a guess about which holds
   * the current policy.
   */
  public Path clonePath() {
    if (useFixedPath) {
      Path fixed = base.resolve(prefix);
      return Files.exists(fixed) ? fixed : null;
    }
    List<Path> found = randomisedSubdirectories();
    return found.size() == 1 ? found.get(0) : null;
  }

  /**
   * R189: a new clone directory, having removed any the last run left.
   *
   * <p>A fixed path is used as it is, with nothing removed, because the whole reason to fix it is
   * that its contents outlive the process.
   */
  public Path createNewClonePath() {
    Path chosen;
    if (useFixedPath) {
      chosen = base.resolve(prefix);
    } else {
      for (Path folder : randomisedSubdirectories()) {
        log.warn(
            "Found previous policy repo clone: {}, removing it to avoid conflicts.", folder);
        try {
          RepoCloner.deleteRecursively(folder);
        } catch (IOException e) {
          log.warn("could not remove {}: {}", folder, e.toString());
        }
      }
      chosen = base.resolve(prefix + "-" + UUID.randomUUID().toString().replace("-", ""));
    }
    try {
      Files.createDirectories(chosen);
    } catch (IOException e) {
      throw new IllegalStateException("could not create " + chosen, e);
    }
    return chosen;
  }

  /** R190: the clone directory, made if it is not there yet. */
  public Path clonePathOrNew() {
    Path existing = clonePath();
    return existing != null ? existing : createNewClonePath();
  }

  private List<Path> randomisedSubdirectories() {
    if (!Files.isDirectory(base)) {
      return List.of();
    }
    List<Path> found = new ArrayList<>();
    // R379: anything matching the pattern counts, directory or not. The source globs, and a file
    // sitting where a clone directory should be is a name already taken — treating it as absent
    // makes the finder hand out a path that cannot be cloned into.
    try (Stream<Path> children = Files.list(base)) {
      children
          .filter(child -> child.getFileName().toString().startsWith(prefix + "-"))
          .forEach(found::add);
    } catch (IOException e) {
      log.warn("could not read {}: {}", base, e.toString());
    }
    found.sort(Path::compareTo);
    return found;
  }
}
