package io.akka.opal.domain;

import akka.javasdk.annotations.TypeName;

/** What happens to one channel's log. SPEC-001 §2.4. */
public sealed interface ChannelEvent {

  @TypeName("change-accepted")
  record ChangeAccepted(Change change) implements ChannelEvent {}

  /**
   * A member is now one of this channel's. Written into the channel's own log rather than
   * worked out from an index built afterwards, so that a change and a member joining are
   * ordered against each other by the same single writer.
   */
  @TypeName("member-enrolled")
  record MemberEnrolled(String memberId) implements ChannelEvent {}
}
