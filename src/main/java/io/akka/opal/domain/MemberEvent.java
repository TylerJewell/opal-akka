package io.akka.opal.domain;

import akka.javasdk.annotations.TypeName;
import java.util.Set;

/** What happens to one member of the fleet. SPEC-001 §2.3. */
public sealed interface MemberEvent {

  @TypeName("member-joined")
  record MemberJoined(String channel, Set<String> watching) implements MemberEvent {}

  /** The change was next in line and addressed somewhere this member watches. */
  @TypeName("change-applied")
  record ChangeApplied(Change change) implements MemberEvent {}

  /**
   * The change was next in line and addressed nowhere this member watches. The position still
   * moves, or the next change this member does watch would look like a gap.
   */
  @TypeName("change-skipped")
  record ChangeSkipped(long position) implements MemberEvent {}

  /** The change was at or behind where this member already is. Kept because a fleet that
   *  redelivers is worth being able to see. */
  @TypeName("duplicate-seen")
  record DuplicateSeen(long position) implements MemberEvent {}

  @TypeName("gap-recorded")
  record GapRecorded(long expected, long received) implements MemberEvent {}

  @TypeName("member-left")
  record MemberLeft() implements MemberEvent {}

  @TypeName("member-returned")
  record MemberReturned() implements MemberEvent {}
}
