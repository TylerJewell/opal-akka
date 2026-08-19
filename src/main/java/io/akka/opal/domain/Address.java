package io.akka.opal.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Where a change is addressed. SPEC-001 §2.1 and rules 1-4.
 *
 * <p>A name is segments joined by {@code /}, optionally preceded by a prefix and a {@code :}. It
 * expands to itself and every name above it, and to nothing below it, so a change reaches a member
 * watching that name or any name above it. Where a {@code :} is present, only the right-most one
 * separates prefix from name, and the prefix is put back on every expansion.
 *
 * <p>The table this reproduces was measured against the original, not read from it —
 * {@code opal-port/probes/probe_02_topic_fanout.py}, question-log rows 3 and 4.
 */
public record Address(String name) {

  private static final String SEGMENT_DELIMITER = "/";
  private static final char PREFIX_DELIMITER = ':';

  public Address {
    if (name == null) {
      throw new IllegalArgumentException("an address needs a name");
    }
  }

  public static Address of(String name) {
    return new Address(name);
  }

  /** This name and every name above it, outermost first. Never empty. */
  public List<String> expand() {
    var colon = name.lastIndexOf(PREFIX_DELIMITER);
    // A colon with nothing before it marks no prefix, and goes away with the split:
    // ":users" is the name "users". Measured against the original, not chosen here.
    var prefix = colon <= 0 ? "" : name.substring(0, colon + 1);
    var bare = colon < 0 ? name : name.substring(colon + 1);

    var expanded = new ArrayList<String>();
    var running = new StringBuilder();
    for (var segment : bare.split(SEGMENT_DELIMITER, -1)) {
      if (!expanded.isEmpty()) {
        running.append(SEGMENT_DELIMITER);
      }
      running.append(segment);
      expanded.add(prefix + running);
    }
    return List.copyOf(expanded);
  }

  /** The order this address belongs to: the outermost name it expands to. */
  public String channel() {
    return expand().get(0);
  }

  /** Whether a change on this address reaches a member watching any of {@code watched}. */
  public boolean reaches(Collection<String> watched) {
    for (var name : expand()) {
      if (watched.contains(name)) {
        return true;
      }
    }
    return false;
  }
}
