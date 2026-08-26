package io.akka.opal.common.sources;

import io.akka.opal.common.git.BranchTracker;
import io.akka.opal.common.git.RepoCloner;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A git repository watched for new commits on one branch — SPEC-002 R37. */
public final class GitPolicySource extends PolicySource {

  private static final Logger log = LoggerFactory.getLogger(GitPolicySource.class);

  private final RepoCloner cloner;
  private final String branchName;

  private Git git;
  private BranchTracker tracker;

  public GitPolicySource(
      String remoteSourceUrl,
      String localClonePath,
      String branchName,
      String sshKey,
      int pollingInterval,
      int requestTimeout) {
    super(remoteSourceUrl, localClonePath, pollingInterval);
    this.branchName = branchName;
    this.cloner =
        new RepoCloner(remoteSourceUrl, this.localClonePath, branchName, sshKey, requestTimeout);
  }

  public Repository repository() {
    return git == null ? null : git.getRepository();
  }

  public BranchTracker tracker() {
    return tracker;
  }

  public boolean ready() {
    return tracker != null;
  }

  @Override
  public void getInitialPolicyStateFromRemote() {
    try {
      git = cloner.cloneOrOpen();
    } catch (RepoCloner.GitFailed e) {
      fireFailure(e);
      return;
    }
    tracker = new BranchTracker(git, branchName, "origin", cloner);
  }

  @Override
  protected void releaseRepository() {
    if (git != null) {
      git.close();
      git = null;
      tracker = null;
    }
  }

  @Override
  public void checkForChanges() {
    if (tracker == null) {
      log.warn("no clone yet, retrying the initial fetch");
      getInitialPolicyStateFromRemote();
      if (tracker == null) {
        return;
      }
    }
    log.info("Pulling changes from remote: 'origin'");
    BranchTracker.PullResult result = tracker.pull();
    if (!result.hasChanges()) {
      log.info("No new commits: HEAD is at '{}'", result.latest().getName());
      return;
    }
    log.info(
        "Found new commits: old HEAD was '{}', new HEAD is '{}'",
        result.previous().getName(),
        result.latest().getName());
    fireNewPolicy(result.previous(), result.latest());
  }
}
