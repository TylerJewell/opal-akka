package io.akka.opal.common.monitoring;

import java.net.URI;
import java.util.List;

/**
 * The trace filter OPAL installs — SPEC-002 R161.
 *
 * <p>A trace whose root span's HTTP route is {@code /} is dropped and every other trace is kept.
 * The route tag is preferred because a framework writes the normalised template there; where it
 * is absent the raw URL is parsed instead, and a URL that will not parse keeps the trace rather
 * than losing it.
 */
final class DropRootPathTraces {

  private DropRootPathTraces() {}

  /** The kept trace, or null to drop it — the same contract the source's filter has. */
  static List<Span> processTrace(List<Span> trace) {
    Span root = null;
    for (Span span : trace) {
      if (span.parentId() == 0) {
        root = span;
        break;
      }
    }
    if (root == null) {
      return trace;
    }
    if ("/".equals(root.getTag("http.route"))) {
      return null;
    }
    String url = root.getTag("http.url");
    if (url != null && !url.isEmpty()) {
      try {
        if ("/".equals(URI.create(url).getPath())) {
          return null;
        }
      } catch (IllegalArgumentException e) {
        // Fail open, as the source does: an unparseable URL keeps the trace.
      }
    }
    return trace;
  }
}
