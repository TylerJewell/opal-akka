package io.akka.opal.application;

import akka.javasdk.NotificationPublisher;
import akka.javasdk.NotificationPublisher.NotificationStream;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.opal.domain.Address;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.ChannelEvent;
import io.akka.opal.domain.ChannelState;
import io.akka.opal.domain.Entry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One channel's log, and the only place a position is handed out. SPEC-001 rules 5-7 and 15-16.
 *
 * <p>A channel has one writer, so two changes can never be given the same position and no position
 * is ever skipped, whoever published them and however close together. That is the whole of the
 * ordering guarantee; everything downstream just has to not lose it.
 */
@Component(id = "channel")
public class ChannelEntity extends EventSourcedEntity<ChannelState, ChannelEvent> {

  /** How many changes a channel keeps, so a member that was away can be handed what it missed. */
  public static final int RETENTION = 1000;

  /**
   * How much room those changes may take between them. A channel's whole state is copied
   * between regions in one piece and has a ceiling, so a count on its own is not a bound:
   * a thousand changes carrying a kilobyte each would go past it.
   */
  public static final int RETAINED_BYTES = 256 * 1024;

  /** A single change may not be larger than what a channel keeps in total. */
  public static final int LARGEST_CHANGE_BYTES = 64 * 1024;

  private final String channel;
  private final NotificationPublisher<Long> accepted;

  public ChannelEntity(EventSourcedEntityContext context, NotificationPublisher<Long> accepted) {
    this.channel = context.entityId();
    this.accepted = accepted;
  }

  public record Publish(String id, String reason, List<Entry> entries) {}

  /**
   * Changes after a position, in position order. {@code complete} is false when the caller is
   * further behind than what is kept, and then the list is empty rather than partial — SPEC-001
   * rule 16, open decision D2.
   */
  public record CatchUp(
      List<Change> changes, boolean complete, long earliestRetained, long currentPosition) {}

  @Override
  public ChannelState emptyState() {
    return ChannelState.empty();
  }

  public Effect<Change> publish(Publish publish) {
    if (publish == null || publish.entries() == null || publish.entries().isEmpty()) {
      return effects().error("a change needs at least one entry");
    }
    for (var entry : publish.entries()) {
      if (entry == null || entry.address() == null || entry.destination() == null) {
        return effects().error("every entry needs an address and a destination");
      }
      var of = Address.of(entry.address()).channel();
      if (!of.equals(channel)) {
        return effects()
            .error(
                "a change belongs to one channel: this one is "
                    + channel
                    + ", and an entry is addressed in "
                    + of);
      }
    }
    var change =
        new Change(
            publish.id() == null || publish.id().isBlank()
                ? UUID.randomUUID().toString()
                : publish.id(),
            channel,
            currentState().nextPosition(),
            publish.reason() == null ? "" : publish.reason(),
            publish.entries(),
            Instant.now());
    var size = ChannelState.weigh(change);
    if (size > LARGEST_CHANGE_BYTES) {
      return effects()
          .error(
              "this change is "
                  + size
                  + " characters and the largest a channel takes is "
                  + LARGEST_CHANGE_BYTES);
    }
    return effects()
        .persist(new ChannelEvent.ChangeAccepted(change))
        .thenReply(
            state -> {
              accepted.publish(change.position());
              return change;
            });
  }

  /**
   * Make this member one of the channel's. Written into the same log the changes go into, so a
   * change accepted after this is fanned out to the member and a change accepted before it is
   * caught up from the log — with no window where it is neither.
   */
  public Effect<ChannelState> enrol(String memberId) {
    if (memberId == null || memberId.isBlank()) {
      return effects().error("a member needs a name to be enrolled");
    }
    if (currentState().members().contains(memberId)) {
      return effects().reply(currentState());
    }
    return effects()
        .persist(new ChannelEvent.MemberEnrolled(memberId))
        .thenReply(state -> state);
  }

  public ReadOnlyEffect<CatchUp> changesSince(long position) {
    var state = currentState();
    var current = state.nextPosition() - 1;
    var earliest = state.earliestRetained();
    if (earliest > 0 && position + 1 < earliest) {
      return effects().reply(new CatchUp(List.of(), false, earliest, current));
    }
    return effects().reply(new CatchUp(state.after(position), true, earliest, current));
  }

  /**
   * A live hint that something was accepted, carrying only the position. A reader follows this
   * and then asks the channel itself for what it has not seen, so a hint that never arrives
   * costs a delay and never a missing change.
   */
  public NotificationStream<Long> accepted() {
    return accepted.stream();
  }

  public ReadOnlyEffect<ChannelState> get() {
    return effects().reply(currentState());
  }

  @Override
  public ChannelState applyEvent(ChannelEvent event) {
    return switch (event) {
      case ChannelEvent.ChangeAccepted accepted1 ->
          currentState().accept(accepted1.change(), RETENTION, RETAINED_BYTES);
      case ChannelEvent.MemberEnrolled enrolled ->
          currentState().enrol(channel, enrolled.memberId());
    };
  }
}
