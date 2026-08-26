package io.akka.opal.server.pubsub;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A publication on the keepalive topic every interval — SPEC-002 R67.
 *
 * <p>What it is for: a backbone connection that carries nothing for an hour is one whose failure
 * nobody has noticed. Publishing on a topic no client subscribes to exercises the connection and
 * makes a broken one visible on the next interval rather than on the next policy change.
 */
public final class BroadcastKeepalive implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(BroadcastKeepalive.class);

  private final ScheduledExecutorService scheduler;

  private BroadcastKeepalive(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * Starts publishing, or answers {@code null} where the configuration says not to.
   *
   * <p>A non-positive interval turns it off, and so does having no backbone: a lone replica
   * publishing to itself on a timer would notify every subscriber of that topic and prove nothing
   * about a connection that is not there.
   */
  public static BroadcastKeepalive start(
      boolean hasBackbone, int intervalSeconds, String topic, Consumer<String> publish) {
    if (!hasBackbone || intervalSeconds <= 0) {
      return null;
    }
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opal-broadcast-keepalive");
              thread.setDaemon(true);
              return thread;
            });
    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            publish.accept(topic);
          } catch (Exception e) {
            log.warn("broadcaster keepalive failed: {}", e.toString());
          }
        },
        intervalSeconds,
        intervalSeconds,
        TimeUnit.SECONDS);
    return new BroadcastKeepalive(scheduler);
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }
}
