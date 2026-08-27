package io.akka.opal.common.git;

import java.util.List;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where a branch's head is, and whether a pull moved it — SPEC-002 R37.
 *
 * <p>The previous head is remembered here rather than read back off the repository, because the
 * question is "did anything change since I last looked", and the repository only knows where the
 * branch is now.
 */
public final class BranchTracker {

  private static final Logger log = LoggerFactory.getLogger(BranchTracker.class);

  /** What a pull found: whether anything moved, and the two commits either side. */
  public record PullResult(boolean hasChanges, ObjectId previous, ObjectId latest) {}

  private static final int ATTEMPTS = 2;
  private static final long RETRY_WAIT_MILLIS = 3000;

  private final Git git;
  private final String branchName;
  private final String remoteName;
  private final RepoCloner cloner;

  private ObjectId previousCommit;

  public BranchTracker(Git git, String branchName, String remoteName, RepoCloner cloner) {
    this.git = git;
    this.branchName = branchName;
    this.remoteName = remoteName == null ? "origin" : remoteName;
    this.cloner = cloner;
    checkout();
    this.previousCommit = latestCommit();
  }

  public Git git() {
    return git;
  }

  public ObjectId previousCommit() {
    return previousCommit;
  }

  /**
   * The head of the tracked branch.
   *
   * <p>Read from {@code refs/heads} by its full name rather than by a search: the source reads
   * the repository's local heads, so a branch that exists only as a remote-tracking ref or as a
   * tag is not this branch and the answer is the same failure as for one that is absent.
   */
  public ObjectId latestCommit() {
    try {
      Ref ref = git.getRepository().findRef("refs/heads/" + branchName);
      if (ref == null) {
        log.error("did not find main branch: {}, instead found: {}", branchName, heads());
        throw new RepoCloner.GitFailed("no such branch: " + branchName);
      }
      return ref.getObjectId();
    } catch (RepoCloner.GitFailed e) {
      throw e;
    } catch (Exception e) {
      throw new RepoCloner.GitFailed("could not read branch " + branchName, e);
    }
  }

  /** The heads as the source lists them in its own failure line: a name and a path each. */
  private List<String> heads() {
    try {
      return git.branchList().call().stream()
          .map(ref -> "{'name': '" + Repository.shortenRefName(ref.getName())
              + "', 'path': '" + ref.getName() + "'}")
          .toList();
    } catch (Exception e) {
      return List.of();
    }
  }

  /**
   * R276: the remote is resolved before it is used, and its absence names the remotes there are.
   *
   * <p>Handing an unknown remote name to the library gives back its own message, which does not
   * say what the repository actually has — and that list is the whole value of the line.
   */
  private void requireRemote() {
    java.util.Set<String> remotes = git.getRepository().getRemoteNames();
    if (!remotes.contains(remoteName)) {
      log.error("did not find main branch: no such remote {}, instead found: {}",
          remoteName, remotes.stream().sorted().toList());
      throw new RepoCloner.GitFailed("no such remote: " + remoteName);
    }
  }

  public void checkout() {
    Exception last = null;
    for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
      try {
        git.checkout().setName(branchName).call();
        return;
      } catch (Exception e) {
        last = e;
        if (attempt < ATTEMPTS - 1) {
          sleep();
        }
      }
    }
    log.error(
        "did not find main branch: {}, instead found: {}, got error: {}",
        branchName,
        heads(),
        last == null ? "" : last.toString());
    throw new RepoCloner.GitFailed("could not check out " + branchName, last);
  }

  /** R37: pull, then compare the head against the one recorded last time. */
  public PullResult pull() {
    doPull();
    ObjectId latest = latestCommit();
    if (previousCommit.equals(latest)) {
      return new PullResult(false, previousCommit, previousCommit);
    }
    ObjectId previous = previousCommit;
    previousCommit = latest;
    return new PullResult(true, previous, latest);
  }

  private void doPull() {
    requireRemote();
    Exception last = null;
    for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
      try {
        PullCommand command = git.pull().setRemote(remoteName).setRemoteBranchName(branchName);
        if (cloner != null) {
          cloner.applyTransport(command::setTransportConfigCallback);
        }
        command.call();
        return;
      } catch (Exception e) {
        last = e;
        if (attempt < ATTEMPTS - 1) {
          sleep();
        }
      }
    }
    throw new RepoCloner.GitFailed("could not pull from " + remoteName, last);
  }

  private static void sleep() {
    try {
      Thread.sleep(RETRY_WAIT_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
