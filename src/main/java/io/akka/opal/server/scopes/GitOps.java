package io.akka.opal.server.scopes;

import io.akka.opal.common.metrics.Metrics;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What bounds a fleet's git operations against everybody else's servers — SPEC-002 R219–R226.
 *
 * <p>A server holding a thousand scopes across a hundred repositories will, without any of this,
 * attempt every one of them on every pass, keep attempting the dead ones forever, and start an
 * unbounded number of threads against remotes that never answer. Four things stop that: one
 * operation at a time per source, a ceiling on how many run at once, a timeout that abandons the
 * wait, and a growing delay before a source that keeps failing is tried again.
 *
 * <p>The timeout is soft, and deliberately so. The underlying git call is on its own thread and
 * cannot be interrupted usefully once it is inside a socket read, so what the timeout ends is the
 * <em>waiting</em>: the pass moves on, the source is skipped, and the abandoned thread is counted
 * against a separate, much higher ceiling until it either finishes or the process ends.
 */
public final class GitOps {

  private static final Logger log = LoggerFactory.getLogger(GitOps.class);

  /**
   * R222: how far the doubling is allowed to go.
   *
   * <p>Not a policy so much as arithmetic: past about a thousand doublings the multiplication
   * overflows, and it would do so inside the handler dealing with the failure. At sixty-four
   * doublings from a ten-second base the delay is longer than the age of the universe, so the
   * clamp changes no reachable answer.
   */
  static final int MAX_BACKOFF_DOUBLINGS = 64;

  /** R223: past this delay a source is, in practice, waiting for somebody to intervene. */
  static final double BACKOFF_ABANDONED_SECONDS = 24 * 3600.0;

  /** What a source's repeated failures have earned it. */
  record SourceBackoff(int consecutiveFailures, double nextAttemptAtSeconds, String lastError) {}

  private static final Set<String> BUSY = ConcurrentHashMap.newKeySet();
  private static final Map<String, SourceBackoff> BACKOFF = new ConcurrentHashMap<>();
  private static final AtomicInteger IN_FLIGHT = new AtomicInteger();

  private static volatile Settings settings = Settings.defaults();
  private static volatile Semaphore workers = new Semaphore(10);
  private static volatile Metrics metrics = new Metrics();

  /** The five entries that decide all of this. */
  public record Settings(
      double fetchTimeoutSeconds,
      int maxWorkers,
      int maxZombies,
      double backoffBaseSeconds,
      double backoffMaxSeconds) {

    public static Settings defaults() {
      return new Settings(120.0, 10, 40, 10.0, 0.0);
    }
  }

  private GitOps() {}

  public static void configure(Settings replacement, Metrics registry) {
    settings = replacement == null ? Settings.defaults() : replacement;
    workers = new Semaphore(Math.max(1, settings.maxWorkers()));
    if (registry != null) {
      metrics = registry;
    }
  }

  public static Settings settings() {
    return settings;
  }

  /** R219: whether this source already has an operation running. */
  public static boolean inFlight(String sourceId) {
    return BUSY.contains(sourceId);
  }

  /** R220: how many operations are running or abandoned, across every source. */
  public static int busyCount() {
    return IN_FLIGHT.get();
  }

  /** Raised when the global ceiling refuses a new operation. */
  public static final class ConcurrencyLimitExceeded extends RuntimeException {
    public ConcurrencyLimitExceeded(String message) {
      super(message);
    }
  }

  /** Raised when the wait for an operation ran out. */
  public static final class OperationTimedOut extends RuntimeException {
    public OperationTimedOut(String message) {
      super(message);
    }
  }

  /**
   * R220 and R221: runs one git operation, bounded three ways.
   *
   * <p>Refused outright past the global ceiling; queued behind the worker limit; and abandoned
   * once the timeout passes. An abandoned operation stops holding a worker slot — one hung remote
   * must not consume one for the life of the process — but keeps counting against the ceiling,
   * which is what the ceiling is for.
   */
  public static <T> T run(String sourceId, Supplier<T> operation) {
    int ceiling = Math.max(0, settings.maxZombies());
    if (ceiling > 0 && IN_FLIGHT.get() >= ceiling) {
      metrics.increment(
          "opal_server.scopes.git_ops_refused",
          Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
      throw new ConcurrencyLimitExceeded(
          "in-flight git ops (" + IN_FLIGHT.get() + ") reached SCOPES_GIT_MAX_ZOMBIES ("
              + ceiling + ")");
    }

    BUSY.add(sourceId);
    IN_FLIGHT.incrementAndGet();
    emitInFlight();
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-scope-git");
              thread.setDaemon(true);
              return thread;
            });
    Future<T> future = null;
    boolean abandoned = false;
    try {
      workers.acquire();
      try {
        future = executor.submit(operation::get);
        double timeout = settings.fetchTimeoutSeconds();
        if (timeout > 0) {
          try {
            return future.get((long) (timeout * 1000), TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            abandoned = true;
            throw new OperationTimedOut(
                "git operation on " + sourceId + " did not finish within " + timeout + "s");
          }
        }
        return future.get();
      } finally {
        workers.release();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted running a git operation", e);
    } catch (java.util.concurrent.ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(cause);
    } finally {
      if (abandoned) {
        // The thread is still inside the git call and will not stop when asked; it is left to
        // finish on its own and counted until it does.
        Future<T> lingering = future;
        Thread watcher =
            new Thread(
                () -> {
                  try {
                    lingering.get();
                  } catch (Exception ignored) {
                    // Whatever it ends as, the only thing wanted here is that it ended.
                  } finally {
                    release(sourceId);
                    executor.shutdownNow();
                  }
                },
                "opal-scope-git-lingering");
        watcher.setDaemon(true);
        watcher.start();
      } else {
        release(sourceId);
        executor.shutdownNow();
      }
    }
  }

  private static void release(String sourceId) {
    BUSY.remove(sourceId);
    IN_FLIGHT.decrementAndGet();
    emitInFlight();
  }

  private static void emitInFlight() {
    metrics.gauge(
        "opal_server.scopes.git_ops_in_flight",
        (long) IN_FLIGHT.get(),
        Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
  }

  // -- the per-source backoff ------------------------------------------------

  /** R222: the delay armed after the n-th consecutive failure, counting from one. */
  public static double backoffDelay(int failures) {
    double base = positiveOrZero(settings.backoffBaseSeconds());
    if (base <= 0) {
      return 0;
    }
    double raw = base * Math.pow(2, Math.min(failures - 1, MAX_BACKOFF_DOUBLINGS));
    double cap = positiveOrZero(settings.backoffMaxSeconds());
    if (cap > 0) {
      // A cap below the base would make this silently inert — every delay shorter than the gap
      // to the next pass, so nothing ever skipped. The base is the floor.
      raw = Math.min(raw, Math.max(cap, base));
    }
    return raw;
  }

  static double positiveOrZero(double value) {
    if (!Double.isFinite(value) || value <= 0) {
      return 0;
    }
    return value;
  }

  /** R223: records a failure and arms the next attempt. */
  public static void recordFailure(String sourceId, String url, String error) {
    if (positiveOrZero(settings.backoffBaseSeconds()) <= 0) {
      return;
    }
    SourceBackoff previous = BACKOFF.get(sourceId);
    int failures = previous == null ? 1 : previous.consecutiveFailures() + 1;
    double delay = backoffDelay(failures);
    double now = System.nanoTime() / 1_000_000_000.0;
    BACKOFF.put(sourceId, new SourceBackoff(failures, now + delay, error));
    if (failures == 1) {
      log.warn(
          "{} failed to sync ({}); skipping it for {}s, doubling on each further failure",
          url,
          error,
          delay);
    }
    boolean crossedAbandoned =
        delay >= BACKOFF_ABANDONED_SECONDS
            && (previous == null || backoffDelay(failures - 1) < BACKOFF_ABANDONED_SECONDS);
    if (crossedAbandoned) {
      log.warn(
          "{} has failed {} times in a row; the next attempt is more than a day away",
          url,
          failures);
    }
    emitSourcesInBackoff();
  }

  /** A source that answered is no longer in backoff, whatever it did before. */
  public static void clearFailure(String sourceId) {
    if (BACKOFF.remove(sourceId) != null) {
      emitSourcesInBackoff();
    }
  }

  public static void forgetSource(String sourceId) {
    BACKOFF.remove(sourceId);
  }

  /** R224: whether the periodic pass should skip this source for now. */
  public static boolean inBackoff(String sourceId) {
    if (positiveOrZero(settings.backoffBaseSeconds()) <= 0) {
      return false;
    }
    SourceBackoff entry = BACKOFF.get(sourceId);
    if (entry == null) {
      return false;
    }
    return entry.nextAttemptAtSeconds() > System.nanoTime() / 1_000_000_000.0;
  }

  /** How long is left before this source is attempted again, for the log line that says so. */
  public static double secondsUntilRetry(String sourceId) {
    SourceBackoff entry = BACKOFF.get(sourceId);
    if (entry == null) {
      return 0;
    }
    return Math.max(0, entry.nextAttemptAtSeconds() - System.nanoTime() / 1_000_000_000.0);
  }

  public static int consecutiveFailures(String sourceId) {
    SourceBackoff entry = BACKOFF.get(sourceId);
    return entry == null ? 0 : entry.consecutiveFailures();
  }

  /** R225: how many sources the pass is skipping right now. */
  public static void emitSourcesInBackoff() {
    long live = 0;
    if (positiveOrZero(settings.backoffBaseSeconds()) > 0) {
      double now = System.nanoTime() / 1_000_000_000.0;
      for (SourceBackoff entry : BACKOFF.values()) {
        if (entry.nextAttemptAtSeconds() > now) {
          live++;
        }
      }
    }
    metrics.gauge(
        "opal_server.scopes.sources_in_backoff",
        live,
        Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
  }

  public static void skipped(String reason) {
    metrics.increment("opal_server.scopes.git_op_skipped", Map.of("reason", reason));
  }

  public static void failed(String operation, String reason) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("op", operation);
    tags.put("reason", reason);
    metrics.increment("opal_server.scopes.git_op_failures", tags);
  }

  /** Everything this holds, so one test cannot leave state for the next. */
  public static void reset() {
    BUSY.clear();
    BACKOFF.clear();
    IN_FLIGHT.set(0);
    settings = Settings.defaults();
    workers = new Semaphore(settings.maxWorkers());
  }

  static List<String> busySources() {
    return new ArrayList<>(BUSY);
  }
}
