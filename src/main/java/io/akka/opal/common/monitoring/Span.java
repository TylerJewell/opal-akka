package io.akka.opal.common.monitoring;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One timed operation inside a trace — SPEC-002 R160.
 *
 * <p>The fields are the ones a Datadog agent reads. A span is opened by {@link Apm#trace} and
 * closed by leaving the try-with-resources block that holds it; closing the outermost one hands
 * the whole trace to the filter and then to the agent.
 */
public final class Span implements AutoCloseable {

  private final Apm owner;
  private final Trace trace;
  private final Span parent;

  private final String service;
  private final String name;
  private String resource;
  private String type = "";

  private final long traceId;
  private final long spanId;
  private final long parentId;
  private final long startNanos;
  private final long startWallNanos;

  private long durationNanos;
  private int error;
  private final Map<String, String> meta = new LinkedHashMap<>();
  private final Map<String, Double> numbers = new LinkedHashMap<>();

  Span(Apm owner, Trace trace, Span parent, String service, String name, String resource,
       long traceId, long spanId, long parentId, long startWallNanos) {
    this.owner = owner;
    this.trace = trace;
    this.parent = parent;
    this.service = service;
    this.name = name;
    this.resource = resource;
    this.traceId = traceId;
    this.spanId = spanId;
    this.parentId = parentId;
    this.startWallNanos = startWallNanos;
    this.startNanos = System.nanoTime();
  }

  public Span setTag(String key, String value) {
    if (value != null) {
      meta.put(key, value);
    }
    return this;
  }

  public Span setNumber(String key, double value) {
    numbers.put(key, value);
    return this;
  }

  public Span setResource(String value) {
    this.resource = value;
    return this;
  }

  public Span setType(String value) {
    this.type = value == null ? "" : value;
    return this;
  }

  /** Marks the span as having failed, which is what an agent counts as an error. */
  public Span setError() {
    this.error = 1;
    return this;
  }

  public String getTag(String key) {
    return meta.get(key);
  }

  public boolean isRoot() {
    return parentId == 0;
  }

  String service() {
    return service;
  }

  String name() {
    return name;
  }

  String resource() {
    return resource;
  }

  String type() {
    return type;
  }

  public long traceId() {
    return traceId;
  }

  public long spanId() {
    return spanId;
  }

  public long parentId() {
    return parentId;
  }

  long start() {
    return startWallNanos;
  }

  long duration() {
    return durationNanos;
  }

  int error() {
    return error;
  }

  Map<String, String> meta() {
    return meta;
  }

  Map<String, Double> numbers() {
    return numbers;
  }

  Span parent() {
    return parent;
  }

  Trace traceOf() {
    return trace;
  }

  @Override
  public void close() {
    if (durationNanos == 0) {
      durationNanos = Math.max(1, System.nanoTime() - startNanos);
    }
    owner.finish(this, trace);
  }
}
