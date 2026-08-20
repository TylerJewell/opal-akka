package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.opal.domain.CursorEvent;
import io.akka.opal.domain.CursorState;
import io.akka.opal.domain.Outcome;

/**
 * One subscriber's position in one destination's sequence — SPEC-001 §4 OD-3.
 *
 * <p>Identified by {@code <subscriber>@<destination>}, so each pair is its own single-writer
 * and two destinations never wait on each other (rule 14). This is the trade the port makes
 * against the source, which orders the entries of one publish across destinations and thereby
 * lets a later publish overtake an earlier one at a shared destination (§4 OD-2).
 */
@Component(id = "cursor")
public class CursorEntity extends EventSourcedEntity<CursorState, CursorEvent> {

  private final String subscriber;
  private final String destination;

  public CursorEntity(EventSourcedEntityContext context) {
    Id id = Id.parse(context.entityId());
    this.subscriber = id.subscriber();
    this.destination = id.destination();
  }

  /** The two halves of a cursor's identity, and how they are written as one. */
  public record Id(String subscriber, String destination) {
    public static String of(String subscriber, String destination) {
      return subscriber + "@" + destination;
    }

    public static Id parse(String entityId) {
      int mark = entityId.indexOf('@');
      return new Id(entityId.substring(0, mark), entityId.substring(mark + 1));
    }
  }

  public record Apply(long sequence, String payload) {}

  public record TakeValue(long sequence, String payload) {}

  /** What this subscriber did with the change, and where it now stands. */
  public record Result(Outcome outcome, long applied, boolean hasGap) {}

  @Override
  public CursorState emptyState() {
    return CursorState.empty(subscriber, destination);
  }

  /** Rules 10 to 13. */
  public Effect<Result> apply(Apply command) {
    CursorState state = currentState();
    Outcome outcome = state.decide(command.sequence());

    return switch (outcome) {
      case DUPLICATE -> effects()
          .persist(new CursorEvent.DuplicateIgnored(command.sequence()))
          .thenReply(s -> new Result(Outcome.DUPLICATE, s.applied(), s.hasGap()));
      case APPLIED -> effects()
          .persist(new CursorEvent.ChangeApplied(command.sequence(), command.payload()))
          .thenReply(s -> new Result(Outcome.APPLIED, s.applied(), s.hasGap()));
      case GAP -> effects()
          .persist(
              new CursorEvent.GapRecorded(
                  state.applied() + 1, command.sequence(), command.sequence()))
          .thenReply(s -> new Result(Outcome.GAP, s.applied(), s.hasGap()));
    };
  }

  /** Rule 18. */
  public Effect<Result> takeValue(TakeValue command) {
    return effects()
        .persist(new CursorEvent.ValueTaken(command.sequence(), command.payload()))
        .thenReply(s -> new Result(Outcome.APPLIED, s.applied(), s.hasGap()));
  }

  public ReadOnlyEffect<CursorState> read() {
    return effects().reply(currentState());
  }

  @Override
  public CursorState applyEvent(CursorEvent event) {
    return switch (event) {
      case CursorEvent.ChangeApplied e -> currentState().withApplied(e.sequence(), e.payload());
      case CursorEvent.DuplicateIgnored ignored -> currentState();
      case CursorEvent.GapRecorded e -> currentState().withGap(e.from(), e.to());
      case CursorEvent.ValueTaken e -> currentState().withValueTaken(e.sequence(), e.payload());
    };
  }
}
