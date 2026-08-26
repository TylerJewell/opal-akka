package io.akka.opal.common.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * R159–R162: the tracing {@code ENABLE_DATADOG_APM} turns on.
 *
 * <p>The three things that can be shown false: nothing is traced with it off, a trace whose root
 * is the bare path is dropped and every other kept, and what reaches the agent is the payload the
 * agent reads.
 */
class ApmTest {

  @AfterEach
  void stopTracing() {
    Apm.configure(false, "opal-server");
  }

  /** R159: off is off — no span object is even made, so nothing on a hot path pays for it. */
  @Test
  void tracingOffProducesNoSpans() {
    Apm.configure(false, "opal-server");
    assertFalse(Apm.enabled());
    assertNull(Apm.trace("topic_publisher.publish", "['policy_data']"));
    assertNull(Apm.httpSpan("GET", "/healthcheck"));
  }

  /** R161: a trace whose root route is the bare path is dropped, and its neighbours are not. */
  @Test
  void theBarePathIsTheOnlyRouteDropped() throws Exception {
    List<byte[]> payloads = new ArrayList<>();
    CountDownLatch received = new CountDownLatch(1);
    HttpServer agent = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    agent.createContext(
        "/",
        exchange -> {
          try (InputStream body = exchange.getRequestBody()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            body.transferTo(out);
            payloads.add(out.toByteArray());
          }
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
          received.countDown();
        });
    agent.start();
    try {
      Apm.configureForTest("http://127.0.0.1:" + agent.getAddress().getPort(), "opal-server");
      assertTrue(Apm.enabled());

      try (Span dropped = Apm.httpSpan("GET", "/")) {
        assertNotNull(dropped);
        dropped.setTag("http.url", "http://localhost:7002/");
      }
      try (Span kept = Apm.httpSpan("GET", "/healthcheck")) {
        assertNotNull(kept);
        kept.setTag("http.url", "http://localhost:7002/healthcheck");
      }
      Apm.flush();
      assertTrue(received.await(5, TimeUnit.SECONDS), "the agent heard nothing");

      // One payload, and the route it carries is the one that was not dropped.
      String text = new String(payloads.get(0), java.nio.charset.StandardCharsets.ISO_8859_1);
      assertTrue(text.contains("/healthcheck"), "the kept trace is missing");
      // The dropped trace's resource is exactly `GET /`, which is a prefix of the kept one's —
      // so what is looked for is that string with the length byte the encoding puts in front of
      // it, which only a five-character string carries.
      String droppedResource = Character.toString(0xa5) + "GET " + Character.toString(47);
      assertFalse(text.contains(droppedResource), "the dropped trace was sent");
      assertEquals(1, payloads.size());
    } finally {
      agent.stop(0);
    }
  }

  /** R160: a span opened inside another belongs to the same trace and names it as its parent. */
  @Test
  void aNestedSpanKeepsItsParentsTrace() {
    Apm.configureForTest("http://127.0.0.1:1", "opal-server");
    try (Span outer = Apm.httpSpan("POST", "/data/config")) {
      assertNotNull(outer);
      assertTrue(outer.isRoot());
      try (Span inner = Apm.trace("topic_publisher.publish", "['policy_data']")) {
        assertNotNull(inner);
        assertFalse(inner.isRoot());
        assertEquals(outer.traceId(), inner.traceId());
        assertEquals(outer.spanId(), inner.parentId());
      }
    }
  }

  /** R162: the payload is the agent's own form — a string table, then the spans by index. */
  @Test
  void theEncodedPayloadIsTheAgentsForm() {
    Apm.configureForTest("http://127.0.0.1:1", "opal-server");
    Span span;
    try (Span open = Apm.trace("bundle_maker.git_file_read", "rbac.rego")) {
      span = open;
    }
    byte[] payload = TraceAgent.encode(List.of(List.of(span)));
    // [ table, traces ] — a two-element array is `0x92`, and both names are in the table.
    assertEquals((byte) 0x92, payload[0]);
    String text = new String(payload, java.nio.charset.StandardCharsets.ISO_8859_1);
    assertTrue(text.contains("bundle_maker.git_file_read"));
    assertTrue(text.contains("rbac.rego"));
    assertTrue(text.contains("opal-server"));
  }
}
