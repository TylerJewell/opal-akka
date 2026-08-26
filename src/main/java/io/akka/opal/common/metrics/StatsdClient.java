package io.akka.opal.common.metrics;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * The DogStatsD datagrams OPAL sends — SPEC-002 R154–R158.
 *
 * <p>One datagram per call, UTF-8, newline-terminated, sent and forgotten. The format is the one
 * the source's client puts on the wire, recorded by binding a socket under it: a counter is
 * {@code namespace.metric:1|c}, a gauge {@code namespace.metric:<value>|g}, tags follow as
 * {@code |#k:v,k2:v2} in the order they were given, and an event is
 * {@code _e{titleBytes,textBytes}:title|text} with no namespace on it at all.
 *
 * <p>The two lengths in an event header are byte counts of the UTF-8 encoding <em>after</em>
 * newline escaping, not character counts: an eleven-character title with two two-byte characters
 * in it announces 13.
 */
public final class StatsdClient implements AutoCloseable {

  /** Where the client points when nothing configured it. */
  public static final String DEFAULT_HOST = "localhost";

  public static final int DEFAULT_PORT = 8125;

  private final DatagramSocket socket;
  private final InetSocketAddress target;
  private final String namespace;

  public StatsdClient(String host, int port, String namespace) {
    this.namespace = namespace == null || namespace.isEmpty() ? null : namespace;
    this.target = new InetSocketAddress(host, port);
    try {
      this.socket = new DatagramSocket();
    } catch (IOException e) {
      throw new IllegalStateException("could not open a statsd socket", e);
    }
  }

  public String namespace() {
    return namespace;
  }

  public void increment(String metric, Map<String, String> tags) {
    send(counter(namespace, metric, 1, tags));
  }

  public void decrement(String metric, Map<String, String> tags) {
    send(counter(namespace, metric, -1, tags));
  }

  public void gauge(String metric, long value, Map<String, String> tags) {
    send(prefixed(namespace, metric) + ":" + value + "|g" + tagSuffix(tags));
  }

  public void gauge(String metric, double value, Map<String, String> tags) {
    send(prefixed(namespace, metric) + ":" + number(value) + "|g" + tagSuffix(tags));
  }

  public void event(String title, String text, Map<String, String> tags) {
    send(eventLine(title, text, tags));
  }

  static String counter(String namespace, String metric, int delta, Map<String, String> tags) {
    return prefixed(namespace, metric) + ":" + delta + "|c" + tagSuffix(tags);
  }

  /** The datagram an event produces. */
  static String eventLine(String title, String text, Map<String, String> tags) {
    String escapedTitle = escape(title);
    String escapedText = escape(text);
    return "_e{"
        + byteLength(escapedTitle)
        + ","
        + byteLength(escapedText)
        + "}:"
        + escapedTitle
        + "|"
        + escapedText
        + tagSuffix(tags);
  }

  private static String prefixed(String namespace, String metric) {
    return namespace == null || namespace.isEmpty() ? metric : namespace + "." + metric;
  }

  private static String tagSuffix(Map<String, String> tags) {
    if (tags == null || tags.isEmpty()) {
      return "";
    }
    StringBuilder out = new StringBuilder("|#");
    boolean first = true;
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      if (!first) {
        out.append(',');
      }
      out.append(tag.getKey()).append(':').append(tag.getValue());
      first = false;
    }
    return out.toString();
  }

  private static String escape(String content) {
    return content == null ? "" : content.replace("\n", "\\n");
  }

  private static int byteLength(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  /**
   * A double rendered the way the source renders one.
   *
   * <p>Both languages print the shortest text that reads back as the same double; they disagree
   * about when to use exponent notation, and the disagreement covers values a duration in seconds
   * really takes. Plain notation is used across the range the source uses it for.
   */
  static String number(double value) {
    String shortest = Double.toString(value);
    if (shortest.indexOf('E') < 0) {
      return shortest;
    }
    double magnitude = Math.abs(value);
    if (magnitude >= 1e-4 && magnitude < 1e16) {
      String plain = new BigDecimal(shortest).stripTrailingZeros().toPlainString();
      // A whole number keeps its `.0`: the value is a double on both sides, and the source
      // prints one as `12345678.0` rather than as an integer.
      return plain.indexOf('.') < 0 ? plain + ".0" : plain;
    }
    return shortest;
  }

  private void send(String line) {
    byte[] payload = (line + "\n").getBytes(StandardCharsets.UTF_8);
    try {
      socket.send(new DatagramPacket(payload, payload.length, target));
    } catch (IOException e) {
      // Fire and forget, exactly as the source's client is: a metric that cannot be delivered
      // never fails the operation that reported it.
    }
  }

  @Override
  public void close() {
    socket.close();
  }
}
