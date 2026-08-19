package io.akka.opal.domain;

import java.util.Arrays;
import java.util.List;

/**
 * Where a change's value lands in a member's store. SPEC-001 rule 14.
 *
 * <p>A path of segments, so two destinations that merely share a run of characters are different
 * destinations: {@code /user} and {@code /users} are not the same place. The original compares
 * them as strings, which is why a change to one holds up a change to the other — question-log
 * row 7.
 */
public record Destination(List<String> segments) {

  private static final String SEPARATOR = "/";

  public Destination {
    segments = List.copyOf(segments);
  }

  public static Destination of(String path) {
    if (path == null) {
      throw new IllegalArgumentException("a destination needs a path");
    }
    return new Destination(
        Arrays.stream(path.split(SEPARATOR)).filter(s -> !s.isEmpty()).toList());
  }

  public static Destination root() {
    return new Destination(List.of());
  }

  /** The path written one way, so the same place written two ways reads back as one. */
  public String path() {
    return segments.isEmpty() ? SEPARATOR : SEPARATOR + String.join(SEPARATOR, segments);
  }

  /** Whether {@code other} is this destination or sits under it. */
  public boolean contains(Destination other) {
    if (other.segments.size() < segments.size()) {
      return false;
    }
    return other.segments.subList(0, segments.size()).equals(segments);
  }
}
