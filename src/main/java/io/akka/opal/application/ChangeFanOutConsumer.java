package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.consumer.Consumer;
import io.akka.opal.domain.ChannelEvent;

/**
 * Every accepted change, handed to every member of its channel. SPEC-001 rules 8-13.
 *
 * <p>Attached to the channel's log rather than to a stream of its own, because a reader attached to
 * a log is handed events in the order they were written and at least once (question-log row 18).
 * At least once is why a member counts a repeat rather than applying it twice; in order is why a
 * member normally sees the change exactly one past itself.
 *
 * <p>Who belongs to the channel is read from the channel itself rather than from an index built
 * afterwards. A member joining and a change being accepted are then ordered against each other
 * by the same single writer, so there is no window in which a member has joined, a change is
 * accepted, and neither the fan-out nor the catching-up reaches it — SPEC-001 open decision D5.
 */
@Component(id = "change-fan-out")
@Consume.FromEventSourcedEntity(ChannelEntity.class)
public class ChangeFanOutConsumer extends Consumer {

  private final ComponentClient componentClient;
  private final FleetDelivery delivery;

  public ChangeFanOutConsumer(ComponentClient componentClient) {
    this.componentClient = componentClient;
    this.delivery = new FleetDelivery(componentClient);
  }

  public Effect onEvent(ChannelEvent event) {
    return switch (event) {
      case ChannelEvent.ChangeAccepted accepted -> {
        var change = accepted.change();
        var members =
            componentClient
                .forEventSourcedEntity(change.channel())
                .method(ChannelEntity::get)
                .invoke()
                .members();
        for (var member : members) {
          delivery.deliver(member, change);
        }
        yield effects().done();
      }
      case ChannelEvent.MemberEnrolled ignored -> effects().ignore();
    };
  }
}
