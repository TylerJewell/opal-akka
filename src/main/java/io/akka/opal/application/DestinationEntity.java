package io.akka.opal.application;

import akka.javasdk.NotificationPublisher;
import akka.javasdk.NotificationPublisher.NotificationStream;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.DestinationEvent;
import io.akka.opal.domain.DestinationState;
import io.akka.opal.domain.Topics;
import java.util.List;
import java.util.Optional;

/**
 * A destination, and the authority on the order of changes to it — SPEC-001 §4 OD-1.
 *
 * <p>Order has to be decided somewhere. The destination is the only participant that sees
 * every change to itself, so it is where the number is handed out, one command at a time. The
 * source decides nothing here: it takes a lock per destination that records no arrival order
 * (question-log row 7), so which of two changes survives is decided by which one's earlier
 * work finished first (row 9).
 */
@Component(id = "destination")
public class DestinationEntity extends EventSourcedEntity<DestinationState, DestinationEvent> {

  private final String destination;
  private final NotificationPublisher<Change> live;

  public DestinationEntity(EventSourcedEntityContext context, NotificationPublisher<Change> live) {
    this.destination = context.entityId();
    this.live = live;
  }

  /** A change offered to this destination. The sequence and the identity are not the caller's. */
  public record Publish(List<String> topics, String payload, String reason, String batch) {}

  /**
   * An answer to "what have I missed?" — SPEC-001 §3 rules 16 and 18.
   *
   * @param complete false when the gap reaches further back than the retained log, in which
   *     case {@code sequence} and {@code value} are the whole answer and {@code changes} is
   *     not
   */
  public record Span(boolean complete, List<Change> changes, long sequence,
      Optional<String> value) {}

  /**
   * Where a destination has got to, without its log.
   *
   * <p>Separate from {@link #read()} because the two callers want different things: a caller
   * asking "how far behind is the fleet?" wants one number, and answering it with a thousand
   * changes is a megabyte of reply for eight bytes of question.
   */
  public record Summary(String destination, long sequence, Optional<String> value,
      long oldestRetained) {}

  @Override
  public DestinationState emptyState() {
    return DestinationState.empty(destination);
  }

  /** Rules 4, 5, 6, 9. */
  public Effect<Change> publish(Publish command) {
    if (command.topics() == null || command.topics().isEmpty()) {
      return effects().error("a change must be addressed to at least one topic");
    }
    if (command.payload() == null || command.payload().isBlank()) {
      return effects().error("a change must carry a payload");
    }
    if (command.payload().length() > DestinationState.MAX_PAYLOAD_BYTES) {
      // The size, not the payload: an error message travels into logs and back to the
      // caller, and the payload is the caller's own data.
      return effects()
          .error(
              "a change payload of "
                  + command.payload().length()
                  + " characters is over the limit of "
                  + DestinationState.MAX_PAYLOAD_BYTES);
    }

    long sequence = currentState().next();
    Change change =
        new Change(
            Change.identify(destination, sequence),
            destination,
            sequence,
            Topics.expandAll(command.topics()),
            command.payload(),
            command.reason(),
            command.batch());

    return effects()
        .persist(new DestinationEvent.ChangeAccepted(change))
        .thenReply(
            s -> {
              live.publish(change);
              return change;
            });
  }

  /**
   * A live view of this destination's changes, for anyone who wants to watch rather than
   * subscribe.
   *
   * <p>Not the delivery path, and deliberately not: a notification stream starts where the
   * watcher attached and the runtime does not promise every message arrives. Delivery is
   * {@link ChangeConsumer} into {@link CursorEntity}, which does. A watcher that needs to
   * know it saw everything reads the sequence numbers, which is what they are for.
   */
  public NotificationStream<Change> updates() {
    return live.stream();
  }

  public ReadOnlyEffect<Summary> summary() {
    DestinationState state = currentState();
    return effects()
        .reply(
            new Summary(
                state.destination(), state.sequence(), state.value(), state.oldestRetained()));
  }

  public ReadOnlyEffect<Span> since(Long after) {
    DestinationState state = currentState();
    if (!state.canServeFrom(after)) {
      return effects().reply(new Span(false, List.of(), state.sequence(), state.value()));
    }
    return effects()
        .reply(new Span(true, state.since(after), state.sequence(), state.value()));
  }

  @Override
  public DestinationState applyEvent(DestinationEvent event) {
    return switch (event) {
      case DestinationEvent.ChangeAccepted e -> currentState().withAccepted(e.change());
    };
  }
}
