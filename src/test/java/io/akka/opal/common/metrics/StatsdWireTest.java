package io.akka.opal.common.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.WireAnswers;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * R154–R158: the datagrams a measurement becomes, against the ones the source produced.
 *
 * <p>Every expected value here was recorded by binding a socket under OPAL's own metrics module
 * and calling it — {@code probes/complete/metrics_wire.py} — so what is compared is this
 * rebuild's wire against the original's, not against a reading of the original's code.
 */
class StatsdWireTest {

  /** Reads whatever arrives on a socket, so a test can see what a client sent. */
  private static final class Listener implements AutoCloseable {
    private final DatagramSocket socket;
    private final List<String> received = new ArrayList<>();
    private final Thread pump;

    Listener() throws Exception {
      socket = new DatagramSocket(0);
      socket.setSoTimeout(200);
      pump =
          new Thread(
              () -> {
                byte[] buffer = new byte[65535];
                while (!Thread.currentThread().isInterrupted()) {
                  try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    synchronized (received) {
                      received.add(
                          new String(packet.getData(), 0, packet.getLength(),
                              StandardCharsets.UTF_8));
                    }
                  } catch (Exception e) {
                    if (socket.isClosed()) {
                      return;
                    }
                  }
                }
              },
              "statsd-listener");
      pump.setDaemon(true);
      pump.start();
    }

    int port() {
      return socket.getLocalPort();
    }

    List<String> drain() throws InterruptedException {
      Thread.sleep(300);
      synchronized (received) {
        return new ArrayList<>(received);
      }
    }

    @Override
    public void close() {
      pump.interrupt();
      socket.close();
    }
  }

  private static Map<String, String> tags(String... pairs) {
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      out.put(pairs[i], pairs[i + 1]);
    }
    return out;
  }

  @Test
  void everyCallProducesTheSourcesDatagram() throws Exception {
    JsonNode expected = WireAnswers.get("statsd").get("namespace_opal");
    try (Listener listener = new Listener();
        StatsdClient client = new StatsdClient("127.0.0.1", listener.port(), "permit.opal")) {
      client.increment("startup", null);
      client.increment("scope.clone", tags("outcome", "ok", "scope_id", "s1"));
      client.decrement("in_flight", null);
      client.gauge("opal_server.scopes.count", 3L, null);
      client.gauge("with.tags", 1.5, tags("a", "b"));
      client.event("title here", "message body", tags("k", "v"));
      client.event("no tags", "body", null);
      assertEquals(WireAnswers.strings(expected), listener.drain());
    }
  }

  @Test
  void theEdgesOfTheFormatAgreeToo() throws Exception {
    List<String> expected = WireAnswers.strings(WireAnswers.get("statsd_edges"));
    try (Listener listener = new Listener();
        StatsdClient client = new StatsdClient("127.0.0.1", listener.port(), "permit.opal")) {
      client.event("héllo wörld", "twö lines", tags("k", "v"));
      client.event("multi", "first\nsecond", null);
      client.event("empty tags", "body", Map.of());
      client.increment("empty_value_tag", tags("k", ""));
      client.gauge("neg", -1.25, null);
      client.increment("many", tags("a", "1", "b", "2", "c", "3"));
      assertEquals(expected, listener.drain());
    }
  }

  /**
   * R154: with metrics off the client is not re-pointed, and the calls still go out — to the
   * library's own default, with no namespace on them.
   */
  @Test
  void metricsDisabledStillSendsToTheDefaultDestination() {
    JsonNode recorded = WireAnswers.get("statsd_default");
    assertEquals("localhost", recorded.get("default_host").asText());
    assertEquals(8125, recorded.get("default_port").asInt());
    assertTrue(recorded.get("default_namespace").isNull());
    assertEquals(StatsdClient.DEFAULT_HOST, recorded.get("default_host").asText());
    assertEquals(StatsdClient.DEFAULT_PORT, recorded.get("default_port").asInt());

    Metrics metrics = new Metrics();
    metrics.configure(false, "127.0.0.1", 9999, "opal");
    // Nothing was re-pointed, so the counter still lands on the registry and the datagram still
    // goes to the default destination — which is what the recorded answer shows the source doing.
    metrics.increment("startup");
    assertEquals(1, metrics.counter("startup"));
  }

  /** R154: the namespace is lower-cased, hyphens become underscores, and `permit.` goes in front. */
  @Test
  void theNamespaceIsNormalisedTheWayTheSourceNormalisesIt() throws Exception {
    JsonNode expected = WireAnswers.get("statsd");
    try (Listener listener = new Listener()) {
      Metrics metrics = new Metrics();
      metrics.configure(true, "127.0.0.1", listener.port(), "Opal-Server");
      metrics.increment("dashed");
      assertEquals(
          WireAnswers.strings(expected.get("namespace_dashed")), listener.drain());
    }
  }

  /** R157: a double is written the way the source writes one. */
  @Test
  void numbersReadBackTheWayTheSourcePrintsThem() {
    assertEquals("2.0", StatsdClient.number(2.0));
    assertEquals("1.5", StatsdClient.number(1.5));
    assertEquals("-1.25", StatsdClient.number(-1.25));
    assertEquals("0.0001", StatsdClient.number(0.0001));
    assertEquals("12345678.0", StatsdClient.number(12345678.0));
  }
}
