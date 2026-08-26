package io.akka.opal.common.sync;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock held on a file, so that exactly one of several processes does a thing — SPEC-002
 * R194–R196.
 *
 * <p>OPAL uses one to elect a leader among the workers on a machine: whoever takes the lock runs
 * the policy watcher, and the others wait. This matters because the alternative is every worker
 * pulling the same commit and publishing the same update to every client.
 *
 * <p>The lock is released when the holder's process ends, whether or not it released it itself,
 * because the operating system holds it rather than the program. That is what makes a leader that
 * dies replaceable without anybody cleaning up after it.
 */
public final class NamedLock implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(NamedLock.class);

  /** R195: how long to wait before trying again. */
  public static final double DEFAULT_ATTEMPT_INTERVAL_SECONDS = 5.0;

  private final Path path;
  private final double attemptIntervalSeconds;

  private FileChannel channel;
  private FileLock lock;

  public NamedLock(String path) {
    this(path, DEFAULT_ATTEMPT_INTERVAL_SECONDS);
  }

  public NamedLock(String path, double attemptIntervalSeconds) {
    this.path = Path.of(path);
    this.attemptIntervalSeconds = attemptIntervalSeconds;
  }

  /** R194: true only while this object holds the lock. */
  public synchronized boolean isLocked() {
    return lock != null;
  }

  /**
   * R195: waits until the lock is held, or until the timeout runs out.
   *
   * <p>A negative timeout waits for as long as it takes, which is what the leader election does:
   * a worker that is not the leader has nothing else to do with the wait.
   */
  public void acquire(double timeoutSeconds) throws InterruptedException {
    log.debug("[{}] trying to acquire lock (lock={})", ProcessHandle.current().pid(), path);
    long started = System.nanoTime();
    while (true) {
      if (tryAcquire()) {
        log.debug("[{}] lock acquired! (lock={})", ProcessHandle.current().pid(), path);
        return;
      }
      Thread.sleep((long) (attemptIntervalSeconds * 1000));
      double waited = (System.nanoTime() - started) / 1_000_000_000.0;
      if (timeoutSeconds >= 0 && waited > timeoutSeconds) {
        throw new IllegalStateException("could not acquire lock");
      }
    }
  }

  /** R196: takes the lock if it is free, and answers immediately either way. */
  public synchronized boolean tryAcquire() {
    if (lock != null) {
      return true;
    }
    try {
      Path parent = path.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      channel =
          FileChannel.open(
              path,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE);
      lock = channel.tryLock();
      if (lock == null) {
        channel.close();
        channel = null;
        return false;
      }
      return true;
    } catch (IOException | java.nio.channels.OverlappingFileLockException e) {
      closeChannelQuietly();
      return false;
    }
  }

  public synchronized void release() {
    log.debug("[{}] releasing lock (lock={})", ProcessHandle.current().pid(), path);
    try {
      if (lock != null) {
        lock.release();
      }
    } catch (IOException e) {
      log.debug("could not release {}: {}", path, e.toString());
    } finally {
      lock = null;
      closeChannelQuietly();
    }
  }

  private void closeChannelQuietly() {
    try {
      if (channel != null) {
        channel.close();
      }
    } catch (IOException e) {
      log.debug("could not close {}: {}", path, e.toString());
    } finally {
      channel = null;
    }
  }

  @Override
  public void close() {
    release();
  }
}
