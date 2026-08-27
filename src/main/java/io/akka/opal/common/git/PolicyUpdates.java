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
    return createPolicyUpdate(
        repository, oldCommit, newCommit, path -> tracked(path, fileExtensions, bundleIgnore));
  }

  /**
   * The same, with the caller supplying what counts as a policy file.
   *
   * <p>A scope replaces the extension-and-ignore pair entirely rather than adding to it, which
   * is why this takes the whole test and not another list.
   */
  public static Policy.PolicyUpdateMessageNotification createPolicyUpdate(
      Repository repository,
      ObjectId oldCommit,
      ObjectId newCommit,
      java.util.function.Predicate<String> isPolicyFile)
      throws IOException {

    CommitViewer newViewer = new CommitViewer(repository, newCommit);
    if (oldCommit.equals(newCommit)) {
      List<String> paths = new ArrayList<>();
      for (CommitViewer.Node file : newViewer.files()) {
        if (isPolicyFile.test(file.path())) {
          paths.add(file.path());
        }
      }
      return notification(oldCommit, newCommit, paths, repository);
    }

    try (DiffViewer viewer = new DiffViewer(repository, oldCommit, newCommit)) {
      List<String> paths = new ArrayList<>();
      for (String path : viewer.affectedPaths()) {
        if (isPolicyFile.test(path)) {
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

  /**
   * Whether a changed file is one the fleet is told about.
   *
   * <p>No extension list at all is no filter. An <em>empty</em> list is a filter that matches
   * nothing, which is not the same thing: the source's own test is membership of the list, and
   * nothing is a member of an empty one.
   */
  static boolean tracked(String path, List<String> fileExtensions, List<String> ignore) {
    if (fileExtensions == null) {
      return Glob.globStyleMatchPathToList(path, ignore) == null;
    }
    if (!fileExtensions.contains(PurePath.suffix(path))) {
      return false;
    }
    return Glob.globStyleMatchPathToList(path, ignore) == null;
  }

  /**
   * R278: what a <em>scope</em> counts as a policy file.
   *
   * <p>A scope announces changes through a filter of its own rather than through the extension
   * and ignore pair above: a {@code .json} counts only when it is named {@code data.json}, and
   * the scope's {@code bundle_ignore} is not consulted at all — a directory the bundle route
   * would leave out is still a directory whose change the fleet hears about.
   */
  public static boolean isRegoSourceFile(String path, List<String> extensions) {
    List<String> effective = extensions == null ? List.of(".rego", ".json") : extensions;
    String suffix = PurePath.suffix(path);
    if (effective.contains(".json") && ".json".equals(suffix)) {
      return "data.json".equals(PurePath.name(path));
    }
    return effective.contains(suffix);
  }
}
