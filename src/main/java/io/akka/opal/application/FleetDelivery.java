package io.akka.opal.application;

import akka.javasdk.client.ComponentClient;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.Delivery;
import io.akka.opal.domain.MemberReport;

/**
 * Handing changes to one member until it is current. SPEC-001 rules 15-18.
 *
 * <p>Used from two places that look different and are the same thing: a change arriving live, and a
 * member coming back after being away. Both end with the member holding every change up to the
 * channel's current position, applied in order — which is the promise the original does not make
 * (question-log row 9).
 */
public final class FleetDelivery {

  /** Enough passes to close a gap that keeps growing while it is being closed. */
  private static final int PASSES = 20;

  private final ComponentClient componentClient;

  public FleetDelivery(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  /** Hand one change over, and if the member turns out to be behind, hand over what it missed. */
  public Delivery deliver(String memberId, Change change) {
    var outcome = handOver(memberId, change);
    if (outcome.outcome() == Delivery.Outcome.GAP) {
      bringCurrent(memberId, change.channel());
      return handOver(memberId, change);
    }
    return outcome;
  }

  /** Hand over every change this member has not seen, oldest first. */
  public MemberReport bringCurrent(String memberId, String channel) {
    var report = report(memberId);
    for (var pass = 0; pass < PASSES; pass++) {
      var caughtUp =
          componentClient
              .forEventSourcedEntity(channel)
              .method(ChannelEntity::changesSince)
              .invoke(report.position());
      if (!caughtUp.complete() || caughtUp.changes().isEmpty()) {
        return report;
      }
      for (var change : caughtUp.changes()) {
        handOver(memberId, change);
      }
      report = report(memberId);
      if (report.position() >= caughtUp.currentPosition()) {
        return report;
      }
    }
    return report;
  }

  private Delivery handOver(String memberId, Change change) {
    return componentClient
        .forEventSourcedEntity(memberId)
        .method(MemberEntity::deliver)
        .invoke(change);
  }

  private MemberReport report(String memberId) {
    return componentClient.forEventSourcedEntity(memberId).method(MemberEntity::report).invoke();
  }
}
