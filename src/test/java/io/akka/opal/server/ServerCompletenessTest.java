package io.akka.opal.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.common.git.ClonePathFinder;
import io.akka.opal.common.git.RepoCloner;
import io.akka.opal.common.git.Tar;
import io.akka.opal.common.schemas.PolicySource;
import io.akka.opal.common.sync.NamedLock;
import io.akka.opal.server.scopes.GitOps;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The server-side behaviours the completeness survey found missing — SPEC-002 R188–R196 and
 * R219–R233.
 *
 * <p>Each is something a deployment could set and get nothing for, or something that decides what
 * a fleet does to somebody else's git host when a repository stops answering.
 */
class ServerCompletenessTest {

  @AfterEach
  void clearGitState() {
    GitOps.reset();
  }

  // -- where a clone lives (R188-R190) ---------------------------------------

  @Test
  void aRandomisedCloneDirectoryIsFoundAgainByItsPrefix(@TempDir Path base) {
    ClonePathFinder finder = new ClonePathFinder(base.toString(), "opal_repo_clone", false);
    assertNull(finder.clonePath(), "nothing is there yet");
    Path created = finder.createNewClonePath();
    assertTrue(created.getFileName().toString().startsWith("opal_repo_clone-"));
    assertEquals(created, finder.clonePath(), "a sibling process finds the same one");
  }

  @Test
  void aNewCloneDirectoryRemovesTheOneTheLastRunLeft(@TempDir Path base) throws IOException {
    ClonePathFinder finder = new ClonePathFinder(base.toString(), "opal_repo_clone", false);
    Path stale = finder.createNewClonePath();
    Files.writeString(stale.resolve("marker"), "old");
    Path fresh = finder.createNewClonePath();
    assertNotEquals(stale, fresh);
    assertFalse(Files.exists(stale), "the stale clone was left behind");
  }

  @Test
  void aFixedCloneDirectoryIsReusedAndNothingIsRemoved(@TempDir Path base) throws IOException {
    ClonePathFinder finder = new ClonePathFinder(base.toString(), "opal_repo_clone", true);
    Path first = finder.createNewClonePath();
    Files.writeString(first.resolve("marker"), "kept");
    assertEquals(first, finder.clonePath());
    assertEquals(first, finder.createNewClonePath());
    assertTrue(Files.exists(first.resolve("marker")), "a fixed path is not cleared");
  }

  @Test
  void anEmptyBaseOrPrefixIsRefused(@TempDir Path base) {
    assertThrows(IllegalArgumentException.class, () -> new ClonePathFinder("", "p", false));
    assertThrows(
        IllegalArgumentException.class, () -> new ClonePathFinder(base.toString(), "", false));
  }

  // -- the leader lock (R194-R196) -------------------------------------------

  @Test
  void onlyOneHolderGetsTheLock(@TempDir Path base) {
    Path file = base.resolve("leader.lock");
    NamedLock first = new NamedLock(file.toString());
    NamedLock second = new NamedLock(file.toString());
    try {
      assertTrue(first.tryAcquire());
      assertTrue(first.isLocked());
      assertFalse(second.tryAcquire(), "two holders at once");
      first.release();
      assertFalse(first.isLocked());
      assertTrue(second.tryAcquire(), "the lock did not come free");
    } finally {
      first.release();
      second.release();
    }
  }

  // -- what bounds a fleet's git operations (R219-R226) ----------------------

  @Test
  void theCeilingRefusesAnOperationRatherThanStartingIt() throws Exception {
    GitOps.configure(new GitOps.Settings(0, 4, 1, 0, 0), null);
    java.util.concurrent.CountDownLatch inside = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    Thread holder =
        new Thread(
            () ->
                GitOps.run(
                    "source-a",
                    () -> {
                      inside.countDown();
                      try {
                        release.await();
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return null;
                    }));
    holder.setDaemon(true);
    holder.start();
    assertTrue(inside.await(5, java.util.concurrent.TimeUnit.SECONDS));
    assertEquals(1, GitOps.busyCount());
    assertTrue(GitOps.inFlight("source-a"));
    assertThrows(
        GitOps.ConcurrencyLimitExceeded.class, () -> GitOps.run("source-b", () -> null));
    release.countDown();
    holder.join(5000);
  }

  @Test
  void theTimeoutAbandonsTheWaitAndKeepsCountingTheOperation() {
    GitOps.configure(new GitOps.Settings(0.2, 4, 0, 0, 0), null);
    assertThrows(
        GitOps.OperationTimedOut.class,
        () ->
            GitOps.run(
                "slow",
                () -> {
                  try {
                    Thread.sleep(3000);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  return null;
                }));
  }

  /** R222: the delay doubles from the base, and a cap below the base is floored at it. */
  @Test
  void theBackoffDoublesAndRespectsItsCap() {
    GitOps.configure(new GitOps.Settings(0, 4, 0, 10, 0), null);
    assertEquals(10.0, GitOps.backoffDelay(1));
    assertEquals(20.0, GitOps.backoffDelay(2));
    assertEquals(40.0, GitOps.backoffDelay(3));

    GitOps.configure(new GitOps.Settings(0, 4, 0, 10, 25), null);
    assertEquals(20.0, GitOps.backoffDelay(2));
    assertEquals(25.0, GitOps.backoffDelay(3), "the cap applies");

    GitOps.configure(new GitOps.Settings(0, 4, 0, 10, 3), null);
    assertEquals(10.0, GitOps.backoffDelay(5), "a cap under the base is floored at the base");

    GitOps.configure(new GitOps.Settings(0, 4, 0, 0, 0), null);
    assertEquals(0.0, GitOps.backoffDelay(1), "zero disables it");
  }

  /** R224: a failing source is skipped until its delay passes, and a success clears it. */
  @Test
  void aFailingSourceIsSkippedAndASuccessClearsIt() {
    GitOps.configure(new GitOps.Settings(0, 4, 0, 60, 0), null);
    assertFalse(GitOps.inBackoff("s"));
    GitOps.recordFailure("s", "https://example/repo", "boom");
    assertTrue(GitOps.inBackoff("s"));
    assertEquals(1, GitOps.consecutiveFailures("s"));
    GitOps.recordFailure("s", "https://example/repo", "boom");
    assertEquals(2, GitOps.consecutiveFailures("s"));
    GitOps.clearFailure("s");
    assertFalse(GitOps.inBackoff("s"));
    assertEquals(0, GitOps.consecutiveFailures("s"));
  }

  /** R224: with the backoff off, nothing is recorded and nothing is skipped. */
  @Test
  void theBackoffCanBeTurnedOff() {
    GitOps.configure(new GitOps.Settings(0, 4, 0, 0, 0), null);
    GitOps.recordFailure("s", "https://example/repo", "boom");
    assertFalse(GitOps.inBackoff("s"));
  }

  // -- which clone a scope belongs to (R227) ---------------------------------

  /**
   * R227: the shard is chosen from an unsigned byte.
   *
   * <p>Read as a signed value the index is wrong for every branch whose digest starts past 127,
   * and two servers configured alike then clone one repository into two directories.
   */
  @Test
  void theShardIndexReadsTheDigestByteAsUnsigned() {
    for (String branch : List.of("main", "master", "release", "dev", "feature/x", "v2")) {
      PolicySource.GitPolicyScopeSource source =
          new PolicySource.GitPolicyScopeSource(
              "git", "https://example/repo", PolicySource.NoAuthData.get(),
              List.of("."), List.of(".rego"), null, ".manifest", true, branch);
      String id = io.akka.opal.server.scopes.GitPolicyFetcher.sourceId(source, 8);
      int index = Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
      assertTrue(index >= 0 && index < 8, branch + " landed on shard " + index);
    }
  }

  // -- what a bundle archive may contain (R191) ------------------------------

  @Test
  void aTarMemberThatIsALinkIsRefused() {
    byte[] archive = tarMember("evil", '1', "../../etc/passwd");
    IOException raised =
        assertThrows(
            IOException.class,
            () -> Tar.forEach(new ByteArrayInputStream(archive), (name, dir, size, body) -> {}));
    assertTrue(raised.getMessage().contains("directory traversal via link"), raised.getMessage());
  }

  @Test
  void aTarMemberThatIsADeviceIsRefused() {
    byte[] archive = tarMember("dev", '3', "");
    IOException raised =
        assertThrows(
            IOException.class,
            () -> Tar.forEach(new ByteArrayInputStream(archive), (name, dir, size, body) -> {}));
    assertTrue(raised.getMessage().contains("isblk() or ischr()"), raised.getMessage());
  }

  @Test
  void anOrdinaryTarMemberIsStillRead() throws IOException {
    byte[] archive = tarMember("rbac.rego", '0', "");
    List<String> seen = new java.util.ArrayList<>();
    Tar.forEach(new ByteArrayInputStream(archive), (name, dir, size, body) -> seen.add(name));
    assertEquals(List.of("rbac.rego"), seen);
  }

  /** One tar header block, with the type flag and link name the test is about. */
  private static byte[] tarMember(String name, char typeFlag, String linkName) {
    byte[] block = new byte[1024];
    write(block, 0, name);
    write(block, 100, "0000644");
    write(block, 124, "00000000000");
    write(block, 136, "00000000000");
    block[156] = (byte) typeFlag;
    write(block, 157, linkName);
    write(block, 257, "ustar");
    int checksum = 0;
    for (int i = 148; i < 156; i++) {
      block[i] = ' ';
    }
    for (byte b : block) {
      checksum += b & 0xff;
    }
    write(block, 148, String.format("%06o", checksum) + "\0 ");
    return block;
  }

  private static void write(byte[] block, int offset, String value) {
    byte[] raw = value.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(raw, 0, block, offset, raw.length);
  }

  // -- the ssh key file, and where a transport is applied (R185-R186) --------

  @Test
  void anSshKeyIsWrittenWhereTheEntrySaysAndOnlyForAnSshRemote(@TempDir Path base) {
    assertTrue(RepoCloner.isSshUrl("ssh://git@example/repo.git"));
    assertTrue(RepoCloner.isSshUrl("git@example:repo.git"));
    assertFalse(RepoCloner.isSshUrl("https://example/repo.git"));

    Path keyFile = base.resolve("keys").resolve("opal_repo_ssh_key");
    RepoCloner.configureSshKeyFile(keyFile.toString());
    RepoCloner cloner =
        new RepoCloner("ssh://git@example/repo.git", base.resolve("clone").toString(), "main",
            "-----BEGIN KEY-----_abc_-----END KEY-----", 0);
    cloner.applyTransport(callback -> {});
    assertTrue(Files.isRegularFile(keyFile), "the key was not written where the entry says");
    String written = readQuietly(keyFile);
    assertTrue(written.contains("\n"), "the underscores did not become newlines");
    assertFalse(written.contains("_"), written);
  }

  private static String readQuietly(Path path) {
    try {
      return Files.readString(path, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
