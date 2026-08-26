package io.akka.opal.client.store;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samples whether the policy engine answers at all — SPEC-002 R89.
 *
 * <p>Without it, a client whose transactions all succeeded earlier reports itself healthy after
 * the engine has gone away, because nothing has tried to write to it since. The first sample is
 * taken before the loop starts, so the flag reflects the engine rather than its optimistic
 * default from the first moment anything reads it.
 */
public final class LivenessProbe implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(LivenessProbe.class);

  private final String label;
  private final String healthUrl;
  private final HttpClient http;
  private final Duration timeout;
  private final Duration interval;
  private final Consumer<Boolean> setReachable;
  private final BooleanSupplier getReachable;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "opal-liveness-probe");
            thread.setDaemon(true);
            return thread;
          });

  private ScheduledFuture<?> task;

  public LivenessProbe(
      String label,
      String healthUrl,
      HttpClient http,
      int timeoutSeconds,
      int intervalSeconds,
      Consumer<Boolean> setReachable,
      BooleanSupplier getReachable) {
    this.label = label;
    this.healthUrl = healthUrl;
    this.http = http;
    this.timeout = Duration.ofSeconds(Math.max(1, timeoutSeconds));
    this.interval = Duration.ofSeconds(Math.max(1, intervalSeconds));
    this.setReachable = setReachable;
    this.getReachable = getReachable;
  }

  public synchronized void start() {
    if (task != null && !task.isDone()) {
      return;
    }
    boolean initial = sample();
    setReachable.accept(initial);
    task =
        scheduler.scheduleWithFixedDelay(
            this::tick, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    log.info(
        "Started {} liveness probe (interval={}s, timeout={}s, initial_reachable={})",
        label,
        interval.toSeconds(),
        timeout.toSeconds(),
        initial);
  }

  private void tick() {
    boolean reachable = sample();
    if (reachable != getReachable.getAsBoolean()) {
      log.info("{} reachability changed to {}", label, reachable);
      setReachable.accept(reachable);
    }
  }

  /**
   * R261: one reading, with the two kinds of failure told apart.
   *
   * <p>A refused connection or a timeout is what an engine that is restarting looks like and is
   * expected; anything else is not, and a probe that logged neither left an operator watching a
   * client report its engine unreachable with nothing anywhere saying why.
   */
  boolean sample() {
    try {
      HttpResponse<Void> response =
          http.send(
              HttpRequest.newBuilder(URI.create(healthUrl)).timeout(timeout).GET().build(),
              HttpResponse.BodyHandlers.discarding());
      return response.statusCode() >= 200 && response.statusCode() < 300;
    } catch (java.io.IOException e) {
      log.debug("{} is not answering: {}", label, e.toString());
      return false;
    } catch (Exception e) {
      log.warn("{} probe failed unexpectedly, treating engine as unreachable", label, e);
      return false;
    }
  }

  @Override
  public synchronized void close() {
    if (task != null) {
      task.cancel(true);
      task = null;
    }
    scheduler.shutdownNow();
  }
}
