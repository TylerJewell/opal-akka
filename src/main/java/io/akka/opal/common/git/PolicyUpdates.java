package io.akka.opal.common.git;

import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.topics.Topics;
import io.akka.opal.common.util.Glob;
import io.akka.opal.common.util.Paths2;
import io.akka.opal.common.util.PurePath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** What a policy change announces — SPEC-002 R21, R38 and R39. */
public final class PolicyUpdates {

  private static final Logger log = LoggerFactory.getLogger(PolicyUpdates.class);

  private PolicyUpdates() {}

  /**
   * R38 and R39: two equal commits announce every directory holding a tracked file, and two
   * different commits with no tracked file between them announce nothing at all.
   */
  public static Policy.PolicyUpdateMessageNotification createPolicyUpdate(
      Repository repository,
      ObjectId oldCommit,
      ObjectId newCommit,
      List<String> fileExtensions,
      List<String> bundleIgnore)
      throws IOException {

    CommitViewer newViewer = new CommitViewer(repository, newCommit);
    if (oldCommit.equals(newCommit)) {
      List<String> paths = new ArrayList<>();
      for (CommitViewer.Node file : newViewer.files()) {
        if (tracked(file.path(), fileExtensions, bundleIgnore)) {
          paths.add(file.path());
        }
      }
      return notification(oldCommit, newCommit, paths, repository);
    }

    try (DiffViewer viewer = new DiffViewer(repository, oldCommit, newCommit)) {
      List<String> paths = new ArrayList<>();
      for (String path : viewer.affectedPaths()) {
        if (tracked(path, fileExtensions, bundleIgnore)) {
          paths.add(path);
        }
      }
      if (paths.isEmpty()) {
        log.warn(
            "new commits detected but no tracked files were affected: '{}' -> '{}'",
            oldCommit.getName(),
            newCommit.getName());
        return null;
      }
      return notification(oldCommit, newCommit, paths, repository);
    }
  }

  private static Policy.PolicyUpdateMessageNotification notification(
      ObjectId oldCommit, ObjectId newCommit, List<String> paths, Repository repository)
      throws IOException {
    List<String> directories = Paths2.intermediateDirectories(paths);
    List<String> topics = Topics.policyTopics(directories);
    Policy.PolicyUpdateMessage message =
        new Policy.PolicyUpdateMessage(
            new CommitViewer(repository, oldCommit).hash(),
            new CommitViewer(repository, newCommit).hash(),
            directories);
    return new Policy.PolicyUpdateMessageNotification(message, topics);
  }

  /** An empty extension list is no filter at all, which is not the same as an empty match. */
  private static boolean tracked(String path, List<String> fileExtensions, List<String> ignore) {
    if (fileExtensions == null || fileExtensions.isEmpty()) {
      return true;
    }
    if (!fileExtensions.contains(PurePath.suffix(path))) {
      return false;
    }
    return Glob.globStyleMatchPathToList(path, ignore) == null;
  }
}
