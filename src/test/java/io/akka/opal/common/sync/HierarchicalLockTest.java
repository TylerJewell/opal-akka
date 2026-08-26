package io.akka.opal.common.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** SPEC-002 R47, against the seven pairs the source's own conflict test answered. */
class HierarchicalLockTest {

  @Test
  void conflictsMatchTheSource() {
    for (JsonNode row : SourceAnswers.get("hierarchical_lock_conflicts")) {
      String p1 = row.get("p1").asText();
      String p2 = row.get("p2").asText();
      assertEquals(
          row.get("conflict").asBoolean(),
          HierarchicalLock.isConflicting(p1, p2),
          p1 + " vs " + p2);
    }
  }

  /** Two conflicting paths serialise: the second acquire waits for the first release. */
  @Test
  void aConflictingPathWaits() throws Exception {
    HierarchicalLock lock = new HierarchicalLock();
    lock.acquire("/a", "first");

    AtomicBoolean acquired = new AtomicBoolean();
    CountDownLatch done = new CountDownLatch(1);
    Thread second =
        new Thread(
            () -> {
              try {
                lock.acquire("/a/b", "second");
                acquired.set(true);
                lock.release("/a/b", "second");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
    second.start();
    Thread.sleep(100);
    assertTrue(!acquired.get(), "the descendant must wait while the ancestor is held");

    lock.release("/a", "first");
    assertTrue(done.await(5, TimeUnit.SECONDS));
    assertTrue(acquired.get());
  }

  /** Two unrelated paths do not wait for each other. */
  @Test
  void anUnrelatedPathDoesNotWait() throws Exception {
    HierarchicalLock lock = new HierarchicalLock();
    lock.acquire("/a/b", "first");
    lock.acquire("/a/c", "second");
    lock.release("/a/b", "first");
    lock.release("/a/c", "second");
  }

  /** Re-acquiring a path the same holder already has is an error, not a wait on itself. */
  @Test
  void reacquiringOnesOwnPathIsAnError() throws Exception {
    HierarchicalLock lock = new HierarchicalLock();
    lock.acquire("/a", "one");
    assertThrows(IllegalStateException.class, () -> lock.acquire("/a", "one"));
    lock.release("/a", "one");
  }

  @Test
  void releasingSomethingNobodyHoldsIsAnError() {
    HierarchicalLock lock = new HierarchicalLock();
    assertThrows(IllegalStateException.class, () -> lock.release("/a", "one"));
  }
}
