package io.akka.opal.common.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.common.git.RepoCloner;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jgit.api.Git;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * SPEC-002 R37 and R43 — a git repository watched for changes, and the loop that watches it.
 *
 * <p>R43's two halves are separable and only one is obvious. Polling running at all when the
 * interval is positive is one call; the loop surviving a check that raised is the other, and a
 * watcher that stopped on the first transient failure would go quiet for the rest of the
 * deployment's life without anything saying so.
 */
class PolicySourceTest {

  /** A repository with one commit, and a second one added on demand. */
  private static Git repositoryAt(Path where) throws Exception {
    Files.createDirectories(where);
    Git git = Git.init().setDirectory(where.toFile()).setInitialBranch("master").call();
    Files.writeString(where.resolve("rbac.rego"), "package rbac\n\nallow = true\n",
        StandardCharsets.UTF_8);
    git.add().addFilepattern(".").call();
    git.commit().setMessage("first").setAuthor("probe", "p@e").setSign(false).call();
    return git;
  }

  private static void commitAgain(Git git, Path where, String rego) throws Exception {
    Files.writeString(where.resolve("rbac.rego"), rego, StandardCharsets.UTF_8);
    git.add().addFilepattern(".").call();
    git.commit().setMessage("second").setAuthor("probe", "p@e").setSign(false).call();
  }

  /** R37: a clone, then a pull that finds a new head, then the callback naming both commits. */
  @Test
  void aNewCommitFiresTheCallbackWithBothEnds(@TempDir Path work) throws Exception {
    Path origin = work.resolve("origin");
    Git remote = repositoryAt(origin);
    try (remote) {
      GitPolicySource source =
          new GitPolicySource(
              origin.toUri().toString(), work.resolve("clone").toString(), "master", null, 0, 30);

      List<String> announced = Collections.synchronizedList(new java.util.ArrayList<>());
      source.addOnNewPolicyCallback((from, to) -> announced.add(from.name() + "->" + to.name()));

      source.getInitialPolicyStateFromRemote();
      assertTrue(source.ready(), "the clone exists");
      assertEquals(List.of(), announced, "cloning is not a change");

      source.checkForChanges();
      assertEquals(List.of(), announced, "and neither is a pull that found nothing");

      commitAgain(remote, origin, "package rbac\n\nallow = false\n");
      source.checkForChanges();

      assertEquals(1, announced.size(), "one commit, one announcement: " + announced);
      String[] ends = announced.get(0).split("->");
      assertEquals(40, ends[0].length());
      assertEquals(40, ends[1].length());
      assertTrue(!ends[0].equals(ends[1]), "the two ends are different commits");
      source.stop();
    }
  }

  /** R43: with a non-positive interval nothing polls, whatever else is configured. */
  @Test
  void aNonPositiveIntervalRunsNoLoop(@TempDir Path work) throws Exception {
    AtomicInteger checks = new AtomicInteger();
    CountingSource source = new CountingSource(work.resolve("c1").toString(), 0, checks);
    source.run();
    Thread.sleep(300);
    source.stop();
    assertEquals(0, checks.get(), "the initial load is not a check, and nothing polled");
  }

  /** R43: with a positive interval the check runs on it, and a raise does not end the loop. */
  @Test
  void aCheckThatRaisesDoesNotStopThePolling(@TempDir Path work) throws Exception {
    AtomicInteger checks = new AtomicInteger();
    CountDownLatch thrice = new CountDownLatch(3);
    CountingSource source =
        new CountingSource(work.resolve("c2").toString(), 1, checks) {
          @Override
          public void checkForChanges() {
            super.checkForChanges();
            thrice.countDown();
            throw new IllegalStateException("the remote was unreachable");
          }
        };
    source.run();
    boolean reached = thrice.await(15, TimeUnit.SECONDS);
    source.stop();
    assertTrue(reached, "it kept checking after the first two failures, got " + checks.get());
  }

  /** A source that counts its checks and touches no network. */
  private static class CountingSource extends PolicySource {
    private final AtomicInteger checks;

    CountingSource(String clonePath, int pollingInterval, AtomicInteger checks) {
      super("file:///nowhere", clonePath, pollingInterval);
      this.checks = checks;
    }

    @Override
    public void getInitialPolicyStateFromRemote() {}

    @Override
    public void checkForChanges() {
      checks.incrementAndGet();
    }
  }

  /** The clone directory a source is given is the one it writes into. */
  @Test
  void theCloneLandsWhereItWasTold(@TempDir Path work) throws Exception {
    Path origin = work.resolve("origin");
    try (Git remote = repositoryAt(origin)) {
      Path clone = work.resolve("nested").resolve("clone");
      new RepoCloner(origin.toUri().toString(), clone.toString(), "master", null, 30)
          .cloneOrOpen()
          .close();
      assertTrue(Files.isDirectory(clone.resolve(".git")), "a repository, at the given path");
      assertTrue(Files.isRegularFile(clone.resolve("rbac.rego")), "with the tracked file in it");
    }
  }
}
