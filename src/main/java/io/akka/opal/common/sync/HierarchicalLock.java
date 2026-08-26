package io.akka.opal.common.sync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lock over hierarchical paths — SPEC-002 R47.
 *
 * <p>Two paths conflict when either is a prefix of the other, so a write to {@code /a} and a
 * write to {@code /a/b} serialise while {@code /a/b} and {@code /a/c} do not. The test is on the
 * strings rather than on path components, which is why {@code /ab} conflicts with {@code /a}
 * and why the empty path conflicts with everything.
 *
 * <p>Re-acquiring a path already held by the same holder is an error rather than a wait, because
 * a caller that did so would be waiting for itself.
 */
public final class HierarchicalLock {

  private final Set<String> lockedPaths = new HashSet<>();
  private final Map<Object, Set<String>> holderLocks = new HashMap<>();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition released = lock.newCondition();

  /** R47's conflict test, exposed so a probe can run it against the source's. */
  public static boolean isConflicting(String p1, String p2) {
    return p1.equals(p2) || p1.startsWith(p2) || p2.startsWith(p1);
  }

  public void acquire(String path) throws InterruptedException {
    acquire(path, Thread.currentThread());
  }

  public void acquire(String path, Object holder) throws InterruptedException {
    lock.lock();
    try {
      if (holderLocks.getOrDefault(holder, Set.of()).contains(path)) {
        throw new IllegalStateException(
            "Task " + holder + " cannot re-acquire lock on '" + path + "'.");
      }
      while (conflicts(path)) {
        released.await();
      }
      lockedPaths.add(path);
      holderLocks.computeIfAbsent(holder, ignored -> new HashSet<>()).add(path);
    } finally {
      lock.unlock();
    }
  }

  public void release(String path) {
    release(path, Thread.currentThread());
  }

  public void release(String path, Object holder) {
    lock.lock();
    try {
      if (!lockedPaths.contains(path)) {
        throw new IllegalStateException("Cannot release path '" + path + "' that is not locked.");
      }
      Set<String> held = holderLocks.getOrDefault(holder, Set.of());
      if (!held.contains(path)) {
        throw new IllegalStateException(
            "Task " + holder + " cannot release lock on '" + path + "' it does not hold.");
      }
      lockedPaths.remove(path);
      held.remove(path);
      if (held.isEmpty()) {
        holderLocks.remove(holder);
      }
      released.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private boolean conflicts(String path) {
    for (String locked : lockedPaths) {
      if (isConflicting(path, locked)) {
        return true;
      }
    }
    return false;
  }

  /** Runs the body with the path held, releasing it however the body ends. */
  public <T> T withLock(String path, java.util.concurrent.Callable<T> body) throws Exception {
    acquire(path);
    try {
      return body.call();
    } finally {
      release(path);
    }
  }
}
