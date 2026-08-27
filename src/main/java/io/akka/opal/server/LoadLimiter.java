package io.akka.opal.server;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The rate limit on {@code GET /loadlimit} — SPEC-002 R119.
 *
 * <p>It is deliberately global rather than per caller: the point is to stagger a whole fleet's
 * start-up against one server, and a per-caller limit would let every client start at once
 * because each is its own caller. A limit that counted per client would satisfy the notation and
 * do nothing about the thundering herd it exists for.
 *
 * <p>The notation is the {@code limits} library's: {@code "10 per second"},
 * {@code "2/minute"}, {@code "100 per 5 minutes"}.
 */
public final class LoadLimiter {

  private static final Logger log = LoggerFactory.getLogger(LoadLimiter.class);

  private static final Pattern NOTATION =
      Pattern.compile(
          "^\\s*(\\d+)\\s*(?:per|/)\\s*(\\d+)?\\s*"
              + "(second|sec|s|minute|min|m|hour|h|day|d|month|year|y)s?\\s*$",
          Pattern.CASE_INSENSITIVE);

  private final int limit;
  private final Duration window;

  /**
   * The limit as its library spells it — {@code "10 per 1 second"}.
   *
   * <p>This reaches a caller: it is the whole body of the 429, so an abbreviation written in the
   * configuration is expanded here the same way.
   */
  private String canonical = "";
  private final Deque<Long> hits = new ArrayDeque<>();

  public LoadLimiter(String notation) {
    if (notation == null || notation.isBlank()) {
      this.limit = 0;
      this.window = Duration.ZERO;
      return;
    }
    // R280: several limits may be written on one line, separated by a semicolon, a comma or a
    // bar. The library underneath the source returns the first of them from a single parse, and
    // the route the notation configures reads exactly one, so the rest are declared and unused.
    String first = notation.split("[;|,]", 2)[0];
    Matcher matcher = NOTATION.matcher(first);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("could not read a rate limit from: " + notation);
    }
    this.limit = Integer.parseInt(matcher.group(1));
    long multiple = matcher.group(2) == null ? 1 : Long.parseLong(matcher.group(2));
    this.window =
        switch (matcher.group(3).toLowerCase(Locale.ROOT)) {
          case "second", "sec", "s" -> Duration.ofSeconds(multiple);
          case "minute", "min", "m" -> Duration.ofMinutes(multiple);
          case "hour", "h" -> Duration.ofHours(multiple);
          case "month" -> Duration.ofDays(30 * multiple);
          case "year", "y" -> Duration.ofDays(365 * multiple);
          default -> Duration.ofDays(multiple);
        };
    this.canonical = matcher.group(1) + " per " + multiple + " " + unitName(matcher.group(3));
    log.info("rate limiting is on, configured limit: {}", notation);
  }

  public boolean enabled() {
    return limit > 0;
  }

  /** What the 429 says. */
  public String canonicalNotation() {
    return canonical;
  }

  private static String unitName(String unit) {
    return switch (unit.toLowerCase(Locale.ROOT)) {
      case "second", "sec", "s" -> "second";
      case "minute", "min", "m" -> "minute";
      case "hour", "h" -> "hour";
      case "day", "d" -> "day";
      case "month" -> "month";
      default -> "year";
    };
  }

  /** True when the caller is inside the limit; false is the 429. */
  public synchronized boolean allow() {
    if (!enabled()) {
      return true;
    }
    long now = System.nanoTime();
    long cutoff = now - window.toNanos();
    while (!hits.isEmpty() && hits.peekFirst() < cutoff) {
      hits.pollFirst();
    }
    if (hits.size() >= limit) {
      return false;
    }
    hits.addLast(now);
    return true;
  }
}
