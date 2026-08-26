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
              + "(second|sec|s|minute|min|m|hour|h|day|d)s?\\s*$",
          Pattern.CASE_INSENSITIVE);

  private final int limit;
  private final Duration window;
  private final Deque<Long> hits = new ArrayDeque<>();

  public LoadLimiter(String notation) {
    if (notation == null || notation.isBlank()) {
      this.limit = 0;
      this.window = Duration.ZERO;
      return;
    }
    Matcher matcher = NOTATION.matcher(notation);
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
          default -> Duration.ofDays(multiple);
        };
    log.info("rate limiting is on, configured limit: {}", notation);
  }

  public boolean enabled() {
    return limit > 0;
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
