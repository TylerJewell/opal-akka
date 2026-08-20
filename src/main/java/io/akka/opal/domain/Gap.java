package io.akka.opal.domain;

/**
 * A span of sequence numbers a subscriber knows it is missing, both ends included.
 *
 * <p>The span includes the change that revealed the gap: a change arriving above the next
 * expected number is not applied (SPEC-001 §3 rule 12), so it is missing too.
 */
public record Gap(long from, long to) {

  public boolean isEmpty() {
    return from > to;
  }

  /** The same span with everything up to {@code applied} removed. */
  public Gap after(long applied) {
    return new Gap(Math.max(from, applied + 1), to);
  }

  public boolean touches(Gap other) {
    return from <= other.to() + 1 && other.from() <= to + 1;
  }

  public Gap merge(Gap other) {
    return new Gap(Math.min(from, other.from()), Math.max(to, other.to()));
  }
}
