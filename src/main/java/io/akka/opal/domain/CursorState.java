package io.akka.opal.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * One subscriber's position in one destination's sequence — SPEC-001 §2, §3 rules 10 to 13.
 *
 * <p>{@code applied} is the highest change applied with no gap before it, so it only ever
 * moves one step at a time. Everything a subscriber knows it is missing sits in {@code gaps},
 * and {@code gaps} is empty exactly when the subscriber is level with everything it has been
 * handed.
 *
 * <p>{@code gaps} holds at most one span, and holding it as a list is what makes that
 * checkable rather than assumed: a change is only ever refused for being ahead of the next
 * expected one, so every span starts at {@code applied + 1} and any two of them touch.
 *
 * @param value what this subscriber currently holds; absent until it applies its first change
 */
public record CursorState(
    String subscriber, String destination, long applied, List<Gap> gaps, Optional<String> value) {

  public static CursorState empty(String subscriber, String destination) {
    return new CursorState(subscriber, destination, 0, List.of(), Optional.empty());
  }

  /** What this subscriber would do with a change at {@code sequence} — rules 11 to 13. */
  public Outcome decide(long sequence) {
    if (sequence <= applied) {
      return Outcome.DUPLICATE;
    }
    return sequence == applied + 1 ? Outcome.APPLIED : Outcome.GAP;
  }

  /**
   * Rule 13: the value and the position move together. A state where one moved and the other
   * did not is not reachable, because both come from one event.
   */
  public CursorState withApplied(long sequence, String payload) {
    return new CursorState(
        subscriber, destination, sequence, trimmed(sequence), Optional.of(payload));
  }

  /** Rule 12: record the span from the next expected change up to and including the arrival. */
  public CursorState withGap(long from, long to) {
    Gap gap = new Gap(from, to);
    List<Gap> merged = new ArrayList<>();
    for (Gap existing : gaps) {
      if (existing.touches(gap)) {
        gap = gap.merge(existing);
      } else {
        merged.add(existing);
      }
    }
    merged.add(gap);
    merged.sort((a, b) -> Long.compare(a.from(), b.from()));
    return new CursorState(subscriber, destination, applied, List.copyOf(merged), value);
  }

  /** Rule 18: take the destination's current value outright; nothing is missing afterwards. */
  public CursorState withValueTaken(long sequence, String payload) {
    return new CursorState(subscriber, destination, sequence, List.of(), Optional.of(payload));
  }

  public boolean hasGap() {
    return !gaps.isEmpty();
  }

  /** Rule 17: applying up to {@code sequence} clears exactly what that covers. */
  private List<Gap> trimmed(long sequence) {
    List<Gap> kept = new ArrayList<>();
    for (Gap gap : gaps) {
      Gap remaining = gap.after(sequence);
      if (!remaining.isEmpty()) {
        kept.add(remaining);
      }
    }
    return List.copyOf(kept);
  }
}
