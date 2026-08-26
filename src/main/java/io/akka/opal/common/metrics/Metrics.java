package io.akka.opal.common.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The measurements OPAL takes about itself — SPEC-002 R154–R159.
 *
 * <p>Every call goes two places. It goes onto the wire as a DogStatsD datagram, which is what the
 * source does, and it is also recorded here so {@code GET /internal/metrics} can answer with the
 * current values — the one addition this port makes, and the reason it is here is that the wire
 * is write-only and a fleet operator otherwise has no way to ask a single process what it has
 * counted.
 *
 * <p>The emission does not depend on {@code ENABLE_METRICS}. The source's {@code
 * configure_metrics} returns before it points the client anywhere when metrics are off, and the
 * four emitting functions call the client regardless — so an OPAL with metrics disabled still
 * sends every datagram, to the client's own default of {@code localhost:8125} with no namespace
 * on it. Recorded by running it (C-113), and reproduced rather than corrected.
 */
public final class Metrics {

  private static final Logger log = LoggerFactory.getLogger(Metrics.class);

  /** One thing that happened, with the tags OPAL attaches to it. */
  public record Event(String name, String message, Map<String, String> tags) {}

  private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
  private final Map<String, Double> gauges = new ConcurrentHashMap<>();
  private final Map<String, Event> lastEvents = new ConcurrentHashMap<>();
  private final AtomicLong eventCount = new AtomicLong();

  private volatile StatsdClient statsd =
      new StatsdClient(StatsdClient.DEFAULT_HOST, StatsdClient.DEFAULT_PORT, null);

  /**
   * R154: where the datagrams go, and under what name.
   *
   * <p>The namespace is lower-cased with every hyphen turned into an underscore and {@code
   * permit.} put in front of it; an empty one falls back to {@code DD_SERVICE}. With metrics off
   * this returns without re-pointing the client, which is what leaves the datagrams going to the
   * default destination.
   */
  public void configure(boolean enableMetrics, String statsdHost, int statsdPort, String namespace) {
    if (!enableMetrics) {
      log.info("DogStatsD metrics disabled");
      return;
    }
    log.info("DogStatsD metrics enabled; statsd: {}:{}", statsdHost, statsdPort);
    String name = namespace == null || namespace.isEmpty() ? System.getenv("DD_SERVICE") : namespace;
    name = name == null ? "" : name.toLowerCase().replace('-', '_');
    StatsdClient previous = statsd;
    statsd = new StatsdClient(statsdHost, statsdPort, "permit." + name);
    previous.close();
  }

  public void increment(String name) {
    increment(name, null);
  }

  public void increment(String name, Map<String, String> tags) {
    counters.computeIfAbsent(name, ignored -> new AtomicLong()).incrementAndGet();
    statsd.increment(name, tags);
  }

  public void decrement(String name, Map<String, String> tags) {
    counters.computeIfAbsent(name, ignored -> new AtomicLong()).decrementAndGet();
    statsd.decrement(name, tags);
  }

  public void gauge(String name, long value, Map<String, String> tags) {
    gauges.put(name, (double) value);
    statsd.gauge(name, value, tags);
  }

  public void gauge(String name, double value, Map<String, String> tags) {
    gauges.put(name, value);
    statsd.gauge(name, value, tags);
  }

  public void event(String name, String message, Map<String, String> tags) {
    lastEvents.put(name, new Event(name, message, tags == null ? Map.of() : tags));
    eventCount.incrementAndGet();
    statsd.event(name, message, tags);
  }

  public long counter(String name) {
    AtomicLong counter = counters.get(name);
    return counter == null ? 0 : counter.get();
  }

  public Double gaugeValue(String name) {
    return gauges.get(name);
  }

  public Event lastEvent(String name) {
    return lastEvents.get(name);
  }

  /** What the metrics route reports: the counters, the gauges and the last event of each name. */
  public Map<String, Object> snapshot() {
    Map<String, Object> out = new LinkedHashMap<>();
    Map<String, Long> counterValues = new LinkedHashMap<>();
    counters.forEach((name, value) -> counterValues.put(name, value.get()));
    out.put("counters", counterValues);
    out.put("gauges", new LinkedHashMap<>(gauges));
    out.put("events", new LinkedHashMap<>(lastEvents));
    out.put("event_count", eventCount.get());
    return out;
  }
}
