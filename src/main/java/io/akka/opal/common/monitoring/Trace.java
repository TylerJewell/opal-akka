package io.akka.opal.common.monitoring;

import java.util.ArrayList;
import java.util.List;

/** The spans of one trace, collected until the outermost one closes — SPEC-002 R160. */
final class Trace {

  private final List<Span> spans = new ArrayList<>();
  private int open;

  synchronized void opened(Span span) {
    spans.add(span);
    open++;
  }

  /** True when the span just closed was the last one open, so the trace is complete. */
  synchronized boolean closed() {
    open--;
    return open <= 0;
  }

  synchronized List<Span> spans() {
    return List.copyOf(spans);
  }
}
