package io.akka.opal.common.fetcher;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * How many times a fetch is tried, and how long it waits between — SPEC-002 R179–R181.
 *
 * <p>The source wraps every provider's fetch in a retry whose wait is a random time between zero
 * and a ceiling that doubles each attempt, and gives up after two hundred of them. The randomness
 * is the point: a fleet of clients whose data source has just come back does not arrive at it
 * together.
 *
 * <p>An event may carry its own settings, and where it does they replace these entirely rather
 * than adjusting them, which is what {@code set_retry_config} does on the way past.
 */
public final class Retries {

  /** R179: what a provider uses when the event says nothing. */
  public static final int DEFAULT_ATTEMPTS = 200;

  public static final double DEFAULT_MULTIPLIER = 1.0;

  /** The ceiling the source's own wait carries, which no real deployment reaches. */
  public static final double DEFAULT_MAX_WAIT_SECONDS = Long.MAX_VALUE / 2.0;

  /** One retry policy: how many attempts, and the shape of the wait between them. */
  public record Config(int attempts, double multiplier, double maxWaitSeconds) {

    public static Config defaults() {
      return new Config(DEFAULT_ATTEMPTS, DEFAULT_MULTIPLIER, DEFAULT_MAX_WAIT_SECONDS);
    }

    /**
     * R180: the policy an event asked for.
     *
     * <p>A key the event does not carry keeps the default's value, so an event naming only
     * {@code attempts} still waits the way everything else does.
     */
    public static Config from(Map<String, Object> settings) {
      if (settings == null || settings.isEmpty()) {
        return defaults();
      }
      return new Config(
          number(settings, "attempts", DEFAULT_ATTEMPTS).intValue(),
          number(settings, "multiplier", DEFAULT_MULTIPLIER).doubleValue(),
          number(settings, "max", DEFAULT_MAX_WAIT_SECONDS).doubleValue());
    }

    private static Number number(Map<String, Object> settings, String key, double fallback) {
      Object value = settings.get(key);
      return value instanceof Number found ? found : fallback;
    }
  }

  private Retries() {}

  /**
   * R181: the wait before attempt {@code n}, counting the first attempt as one.
   *
   * <p>Uniform between zero and a ceiling that doubles, so the average wait grows while the
   * shortest stays zero — a client that has just failed may try again immediately, and a thousand
   * of them will not.
   */
  public static double waitSeconds(Config config, int attempt) {
    double ceiling = Math.min(config.maxWaitSeconds(), config.multiplier() * Math.pow(2, attempt));
    if (ceiling <= 0) {
      return 0;
    }
    return ThreadLocalRandom.current().nextDouble(0, ceiling);
  }

  /** Runs the work, retrying on any failure, and rethrows the last one when the attempts run out. */
  public static <T> T call(Config config, Supplier<T> work, Sleeper sleeper) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= Math.max(1, config.attempts()); attempt++) {
      try {
        return work.get();
      } catch (RuntimeException e) {
        last = e;
        if (attempt >= config.attempts()) {
          break;
        }
        if (!sleeper.sleep(waitSeconds(config, attempt))) {
          break;
        }
      }
    }
    throw last == null ? new IllegalStateException("no attempt was made") : last;
  }

  /** Waiting, separated so a test can drive two hundred attempts without waiting for any of them. */
  public interface Sleeper {
    /** False to stop retrying — which is what an interrupted wait means. */
    boolean sleep(double seconds);
  }

  /** The wait a running system does. */
  public static final Sleeper REAL =
      seconds -> {
        try {
          Thread.sleep((long) Math.min(seconds * 1000, Integer.MAX_VALUE));
          return true;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      };
}
