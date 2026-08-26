package io.akka.opal.common.git;

import java.util.List;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
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

  public ObjectId latestCommit() {
    try {
      Ref ref = git.getRepository().findRef(branchName);
      if (ref == null) {
        List<String> branches =
            git.branchList().call().stream().map(Ref::getName).toList();
        log.error(
            "did not find main branch: {}, instead found: {}", branchName, branches);
        throw new RepoCloner.GitFailed("no such branch: " + branchName);
      }
      return ref.getObjectId();
    } catch (RepoCloner.GitFailed e) {
      throw e;
    } catch (Exception e) {
      throw new RepoCloner.GitFailed("could not read branch " + branchName, e);
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
        sleep();
      }
    }
    try {
      List<String> branches = git.branchList().call().stream().map(Ref::getName).toList();
      log.error(
          "did not find main branch: {}, instead found: {}, got error: {}",
          branchName,
          branches,
          last == null ? "" : last.toString());
    } catch (Exception ignored) {
      // The branch listing is for the log line only; the failure below is the real answer.
    }
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
        sleep();
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
