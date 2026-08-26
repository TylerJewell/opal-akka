package io.akka.opal.common.git;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;

/**
 * Turns a downloaded OPA bundle into commits in a local repository — SPEC-002 R41.
 *
 * <p>Doing it this way is what lets the same bundle machinery answer {@code GET /policy} whether
 * the policy came from a git remote or from a bundle server: by the time anything reads it, both
 * are a repository with a commit per version.
 *
 * <p>The extraction refuses a member whose path escapes the destination, and refuses a bundle
 * carrying its own {@code .git} — a tar is somebody else's file and both are how it reaches out
 * of the directory it was given.
 */
public final class TarToLocalGit {

  /** Raised when the downloaded archive is not one this may extract. */
  public static final class InvalidBundle extends RuntimeException {
    public InvalidBundle(String message) {
      super(message);
    }
  }

  private static final PersonIdent OPAL =
      new PersonIdent("opal", "opal@opal.ac");

  private final Path localClonePath;
  private final Path tmpBundlePath;
  private final String addPattern;

  public TarToLocalGit(String localClonePath, String tmpBundlePath, String addPattern) {
    this.localClonePath = Path.of(localClonePath);
    this.tmpBundlePath = Path.of(tmpBundlePath);
    this.addPattern = addPattern == null || addPattern.isEmpty() ? "." : addPattern;
  }

  /** The first version: extract, initialise, commit. */
  public Git createLocalGit() throws Exception {
    extractBundleTar();
    Git existing = openIfRepository();
    if (existing == null || existing.branchList().call().isEmpty()) {
      if (existing != null) {
        existing.close();
      }
      return commitLocalGit("Init", true).git();
    }
    return existing;
  }

  /** What a commit produced: the repository and the two commits either side of it. */
  public record Committed(Git git, ObjectId previous, ObjectId latest) {}

  /**
   * A later version. The working tree is replaced wholesale and the {@code .git} directory is
   * carried across, so what git sees is one commit holding every difference — which is what a
   * bundle is.
   */
  public Committed extractBundleToLocalGit(String commitMessage) throws Exception {
    Path backup = Path.of(localClonePath + ".bak");
    RepoCloner.deleteRecursively(backup);
    Files.move(localClonePath, backup, StandardCopyOption.REPLACE_EXISTING);
    try {
      extractBundleTar();
      Files.move(backup.resolve(".git"), localClonePath.resolve(".git"));
    } finally {
      RepoCloner.deleteRecursively(backup);
    }
    return commitLocalGit(commitMessage, false);
  }

  Committed commitLocalGit(String message, boolean shouldInit) throws Exception {
    Git git =
        shouldInit
            ? Git.init().setDirectory(localClonePath.toFile()).call()
            : Git.open(localClonePath.toFile());
    ObjectId previous = null;
    List<Ref> heads = git.branchList().call();
    if (!heads.isEmpty()) {
      previous = git.getRepository().resolve("HEAD");
    }
    git.add().addFilepattern(addPattern).call();
    git.add().setUpdate(true).addFilepattern(addPattern).call();
    ObjectId latest =
        git.commit().setMessage(message).setAuthor(OPAL).setCommitter(OPAL).call().getId();
    return new Committed(git, previous, latest);
  }

  private Git openIfRepository() {
    try {
      return Git.open(localClonePath.toFile());
    } catch (Exception e) {
      return null;
    }
  }

  void extractBundleTar() throws IOException {
    List<String> names = new ArrayList<>();
    Files.createDirectories(localClonePath);
    try (InputStream in =
        new GZIPInputStream(new BufferedInputStream(Files.newInputStream(tmpBundlePath)))) {
      Tar.forEach(
          in,
          (name, isDirectory, size, entryStream) -> {
            names.add(name);
            if (name.equals(".git") || name.startsWith(".git/")) {
              throw new InvalidBundle("No .git files are allowed in OPAL api bundle");
            }
            Path target = localClonePath.resolve(name).normalize();
            if (!target.startsWith(localClonePath)) {
              throw new InvalidBundle("tar member escapes the destination: " + name);
            }
            if (isDirectory) {
              Files.createDirectories(target);
              return;
            }
            Files.createDirectories(target.getParent());
            Files.copy(entryStream, target, StandardCopyOption.REPLACE_EXISTING);
          });
    }
    if (names.isEmpty()) {
      throw new InvalidBundle("No files in bundle");
    }
  }
}
