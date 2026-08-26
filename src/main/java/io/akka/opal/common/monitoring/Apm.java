package io.akka.opal.common.monitoring;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Datadog APM tracing OPAL turns on with {@code ENABLE_DATADOG_APM} — SPEC-002 R159–R162.
 *
 * <p>Off, nothing is traced and nothing is sent, which is the default on both sides. On, every
 * HTTP request the service answers opens a span, the two operations OPAL instruments by hand
 * open one each, and every finished trace goes through {@link DropRootPathTraces} before it is
 * handed to the agent.
 *
 * <p>The tracer is a process-wide singleton because the source's is: {@code configure_apm} is
 * called once during start-up and {@code from ddtrace import tracer} reaches the same object from
 * every module.
 */
public final class Apm {

  private static final Logger log = LoggerFactory.getLogger(Apm.class);

  private static final String RUNTIME_ID = UUID.randomUUID().toString().replace("-", "");
  private static final ThreadLocal<Span> CURRENT = new ThreadLocal<>();

  private static volatile Apm active;

  private final String service;
  private final TraceAgent agent;

  private Apm(String service, TraceAgent agent) {
    this.service = service;
    this.agent = agent;
  }

  /** R159: enable tracing, or say it is off and do nothing. */
  public static synchronized void configure(boolean enableApm, String serviceName) {
    if (!enableApm) {
      log.info("Datadog APM disabled");
      stop();
      return;
    }
    log.info("Enabling Datadog APM");
    stop();
    active = new Apm(serviceName, new TraceAgent(agentUrl()));
  }

  /**
   * Tracing pointed at a named agent, for a test that stands one up.
   *
   * <p>The address otherwise comes from the environment, which a test cannot set for its own
   * process; everything else about this is what {@link #configure} does.
   */
  static synchronized void configureForTest(String agentUrl, String serviceName) {
    stop();
    active = new Apm(serviceName, new TraceAgent(agentUrl));
  }

  /** Where the agent is: the explicit URL, else the agent host, else the local default. */
  static String agentUrl() {
    String url = System.getenv("DD_TRACE_AGENT_URL");
    if (url != null && !url.isEmpty()) {
      return url;
    }
    String host = System.getenv("DD_AGENT_HOST");
    return "http://" + (host == null || host.isEmpty() ? "localhost" : host) + ":8126";
  }

  public static boolean enabled() {
    return active != null;
  }

  /**
   * A span, or null when tracing is off.
   *
   * <p>Null rather than a do-nothing span so that a caller in a hot path pays nothing at all for
   * the default configuration; every call site is a try-with-resources over a nullable.
   */
  public static Span trace(String name, String resource) {
    Apm apm = active;
    if (apm == null) {
      return null;
    }
    Span parent = CURRENT.get();
    Trace trace = parent == null ? new Trace() : parent.traceOf();
    long traceId = parent == null ? randomId() : parent.traceId();
    Span span =
        new Span(
            apm,
            trace,
            parent,
            apm.service,
            name,
            resource,
            traceId,
            randomId(),
            parent == null ? 0 : parent.spanId(),
            System.currentTimeMillis() * 1_000_000L);
    if (parent == null) {
      span.setTag("language", "java");
      span.setTag("runtime-id", RUNTIME_ID);
      span.setTag("_dd.p.dm", "-0");
      span.setNumber("_sampling_priority_v1", 1.0);
      span.setNumber("_dd.top_level", 1.0);
      span.setNumber("process_id", ProcessHandle.current().pid());
    }
    trace.opened(span);
    CURRENT.set(span);
    return span;
  }

  /** The span an answered HTTP request produces, which is what makes the filter have work. */
  public static Span httpSpan(String method, String route) {
    Span span = trace("opal.request", method + " " + route);
    if (span != null) {
      span.setType("web");
      span.setTag("http.method", method);
      span.setTag("http.route", route);
      span.setTag("component", "akka-http");
    }
    return span;
  }

  void finish(Span span, Trace trace) {
    CURRENT.set(span.parent());
    if (!trace.closed()) {
      return;
    }
    List<Span> kept = DropRootPathTraces.processTrace(trace.spans());
    if (kept != null && !kept.isEmpty()) {
      agent.submit(kept);
    }
  }

  /** Sends whatever is buffered and answers the agent's status code. */
  public static int flush() {
    Apm apm = active;
    return apm == null ? 0 : apm.agent.flush();
  }

  static synchronized void stop() {
    Apm apm = active;
    active = null;
    if (apm != null) {
      apm.agent.stop();
    }
  }

  private static long randomId() {
    return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
  }
}
