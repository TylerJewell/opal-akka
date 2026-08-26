package io.akka.opal.common.monitoring;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where finished traces go — SPEC-002 R162.
 *
 * <p>Traces are buffered and flushed once a second on a thread of their own, which is what keeps a
 * request from waiting on the agent. The payload is the agent's v0.5 form: one string table and
 * the spans referring into it by index.
 *
 * <p>The buffer is bounded. A tracer whose agent is not answering drops the oldest traces rather
 * than growing without limit, and says how many it dropped.
 */
final class TraceAgent {

  private static final Logger log = LoggerFactory.getLogger(TraceAgent.class);

  private static final int BUFFER = 1000;
  private static final String TRACER_VERSION = "opal-akka";

  private final URI endpoint;
  private final HttpClient http;
  private final BlockingQueue<List<Span>> pending = new ArrayBlockingQueue<>(BUFFER);
  private final Thread sender;
  private volatile boolean running = true;
  private volatile long dropped;

  TraceAgent(String agentUrl) {
    this.endpoint = URI.create(agentUrl + "/v0.5/traces");
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    this.sender = new Thread(this::run, "opal-apm-sender");
    this.sender.setDaemon(true);
    this.sender.start();
  }

  void submit(List<Span> trace) {
    if (!pending.offer(trace)) {
      dropped++;
    }
  }

  long droppedCount() {
    return dropped;
  }

  /** Sends whatever is buffered now, and answers what the agent said. Used by the tests. */
  int flush() {
    List<List<Span>> batch = new ArrayList<>();
    pending.drainTo(batch);
    if (batch.isEmpty()) {
      return 0;
    }
    return send(batch);
  }

  private void run() {
    while (running) {
      try {
        List<Span> first = pending.poll(1, TimeUnit.SECONDS);
        if (first == null) {
          continue;
        }
        List<List<Span>> batch = new ArrayList<>();
        batch.add(first);
        pending.drainTo(batch);
        send(batch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException e) {
        log.debug("failed to send traces", e);
      }
    }
  }

  private int send(List<List<Span>> batch) {
    byte[] payload = encode(batch);
    HttpRequest request =
        HttpRequest.newBuilder(endpoint)
            .timeout(Duration.ofSeconds(2))
            .header("Content-Type", "application/msgpack")
            .header("Datadog-Meta-Lang", "java")
            .header("Datadog-Meta-Lang-Version", System.getProperty("java.version", "unknown"))
            .header("Datadog-Meta-Lang-Interpreter", System.getProperty("java.vm.name", "unknown"))
            .header("Datadog-Meta-Tracer-Version", TRACER_VERSION)
            .header("Datadog-Client-Computed-Top-Level", "true")
            .header("X-Datadog-Trace-Count", String.valueOf(batch.size()))
            .PUT(HttpRequest.BodyPublishers.ofByteArray(payload))
            .build();
    try {
      HttpResponse<Void> response = http.send(request, HttpResponse.BodyHandlers.discarding());
      return response.statusCode();
    } catch (Exception e) {
      log.debug("failed to send, dropping {} traces to intake at {}", batch.size(), endpoint);
      return 0;
    }
  }

  /** The v0.5 payload: {@code [strings, [[span, ...], ...]]}. */
  static byte[] encode(List<List<Span>> batch) {
    Map<String, Integer> table = new LinkedHashMap<>();
    table.put("", 0);
    for (List<Span> trace : batch) {
      for (Span span : trace) {
        intern(table, span.service());
        intern(table, span.name());
        intern(table, span.resource());
        intern(table, span.type());
        span.meta().forEach((key, value) -> {
          intern(table, key);
          intern(table, value);
        });
        span.numbers().keySet().forEach(key -> intern(table, key));
      }
    }

    MsgPack out = new MsgPack();
    out.array(2);
    out.array(table.size());
    table.keySet().forEach(out::string);
    out.array(batch.size());
    for (List<Span> trace : batch) {
      out.array(trace.size());
      for (Span span : trace) {
        out.array(12);
        out.integer(table.get(span.service()));
        out.integer(table.get(span.name()));
        out.integer(table.get(span.resource()));
        out.integer(span.traceId());
        out.integer(span.spanId());
        out.integer(span.parentId());
        out.integer(span.start());
        out.integer(span.duration());
        out.integer(span.error());
        out.map(span.meta().size());
        for (Map.Entry<String, String> tag : span.meta().entrySet()) {
          out.integer(table.get(tag.getKey()));
          out.integer(table.get(tag.getValue()));
        }
        out.map(span.numbers().size());
        for (Map.Entry<String, Double> number : span.numbers().entrySet()) {
          out.integer(table.get(number.getKey()));
          out.real(number.getValue());
        }
        out.integer(table.get(span.type()));
      }
    }
    return out.bytes();
  }

  private static void intern(Map<String, Integer> table, String value) {
    table.computeIfAbsent(value == null ? "" : value, ignored -> table.size());
  }

  void stop() {
    running = false;
    sender.interrupt();
  }
}
