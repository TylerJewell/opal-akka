package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.Delivery;
import io.akka.opal.domain.MemberEvent;
import io.akka.opal.domain.MemberReport;
import io.akka.opal.domain.MemberState;
import java.util.Set;

/**
 * One member of the fleet. SPEC-001 rules 8-14 and 19.
 *
 * <p>The whole of the ordering guarantee a caller sees is decided here, in three lines: a change
 * at or behind this member's position changes nothing, a change exactly one past it is applied,
 * and a change further ahead is recorded as a gap and left unapplied. Everything else — the
 * catching up, the streaming, the fan-out — exists to keep those three cases the only ones.
 */
@Component(id = "member")
public class MemberEntity extends EventSourcedEntity<MemberState, MemberEvent> {

  private final String memberId;

  public MemberEntity(EventSourcedEntityContext context) {
    this.memberId = context.entityId();
  }

  public record Join(String channel, Set<String> watching) {}

  @Override
  public MemberState emptyState() {
    return MemberState.empty();
  }

  /** Join the fleet, or restate what this member watches. Never resets what it has applied. */
  public Effect<MemberReport> join(Join join) {
    if (join == null || join.channel() == null || join.channel().isBlank()) {
      return effects().error("a member joins one channel, named");
    }
    var mismatched =
        join.watching().stream()
            .map(io.akka.opal.domain.Address::of)
            .map(io.akka.opal.domain.Address::channel)
            .filter(c -> !c.equals(join.channel()))
            .findFirst();
    if (mismatched.isPresent()) {
      return effects()
          .error(
              "a member watches names in one channel: "
                  + join.channel()
                  + " does not hold "
                  + mismatched.get());
    }
    return effects()
        .persist(new MemberEvent.MemberJoined(join.channel(), join.watching()))
        .thenReply(MemberReport::of);
  }

  /** Hand this member one change. */
  public Effect<Delivery> deliver(Change change) {
    if (!currentState().joined()) {
      return effects().error("member " + memberId + " has not joined the fleet");
    }
    var at = currentState().position();
    if (currentState().away()) {
      return effects().reply(Delivery.away(at));
    }
    if (change.position() <= at) {
      return effects()
          .persist(new MemberEvent.DuplicateSeen(change.position()))
          .thenReply(state -> Delivery.duplicate(state.position()));
    }
    if (change.position() > at + 1) {
      return effects()
          .persist(new MemberEvent.GapRecorded(at + 1, change.position()))
          .thenReply(state -> Delivery.gap(state.position(), change.position()));
    }
    if (!change.reaches(currentState().watching())) {
      return effects()
          .persist(new MemberEvent.ChangeSkipped(change.position()))
          .thenReply(state -> Delivery.notWatched(state.position()));
    }
    return effects()
        .persist(new MemberEvent.ChangeApplied(change))
        .thenReply(state -> Delivery.applied(state.position()));
  }

  /** Leave the fleet without losing what has been applied — the outage this port is about. */
  public Effect<MemberReport> leave() {
    if (!currentState().joined()) {
      return effects().error("member " + memberId + " has not joined the fleet");
    }
    if (currentState().away()) {
      return effects().reply(MemberReport.of(currentState()));
    }
    return effects().persist(new MemberEvent.MemberLeft()).thenReply(MemberReport::of);
  }

  /** Come back. What was missed is applied by the caller, from this member's own position. */
  public Effect<MemberReport> comeBack() {
    if (!currentState().joined()) {
      return effects().error("member " + memberId + " has not joined the fleet");
    }
    if (!currentState().away()) {
      return effects().reply(MemberReport.of(currentState()));
    }
    return effects().persist(new MemberEvent.MemberReturned()).thenReply(MemberReport::of);
  }

  public ReadOnlyEffect<MemberReport> report() {
    return effects().reply(MemberReport.of(currentState()));
  }

  @Override
  public MemberState applyEvent(MemberEvent event) {
    return switch (event) {
      case MemberEvent.MemberJoined joined ->
          currentState().join(memberId, joined.channel(), joined.watching());
      case MemberEvent.ChangeApplied applied -> currentState().apply(applied.change());
      case MemberEvent.ChangeSkipped skipped -> currentState().skipTo(skipped.position());
      case MemberEvent.DuplicateSeen ignored -> currentState().countDuplicate();
      case MemberEvent.GapRecorded ignored -> currentState().countGap();
      case MemberEvent.MemberLeft ignored -> currentState().withAway(true);
      case MemberEvent.MemberReturned ignored -> currentState().withAway(false);
    };
  }
}
