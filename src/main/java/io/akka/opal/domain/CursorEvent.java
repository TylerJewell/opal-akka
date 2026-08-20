package io.akka.opal.domain;

import akka.javasdk.annotations.TypeName;

/**
 * What happens to one subscriber's position in one destination's sequence.
 *
 * <p>A refusal is recorded as well as an acceptance. The source drops a change it cannot
 * place without trace (question-log rows 11, 16); here the decision is the record that
 * answers, later, whether the fleet ever converged and when.
 */
public sealed interface CursorEvent {

  @TypeName("change-applied")
  record ChangeApplied(long sequence, String payload) implements CursorEvent {}

  @TypeName("duplicate-ignored")
  record DuplicateIgnored(long sequence) implements CursorEvent {}

  @TypeName("gap-recorded")
  record GapRecorded(long from, long to, long arrived) implements CursorEvent {}

  /** The subscriber took the destination's current value outright, discarding its gaps. */
  @TypeName("value-taken")
  record ValueTaken(long sequence, String payload) implements CursorEvent {}
}
