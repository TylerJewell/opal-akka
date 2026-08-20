package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.consumer.Consumer;
import io.akka.opal.domain.DestinationEvent;

/**
 * Turns each accepted change into a fan-out, in the order the destination accepted them.
 *
 * <p>This is where the port's ordering guarantee is actually cashed in. The runtime delivers
 * a destination's events to this consumer in the order they were written, at least once, so
 * change 5 is never fanned out before change 4. The source has no such step: whoever
 * published simply writes onto a socket, and two changes to one destination race from there
 * (question-log rows 9, 22).
 */
@Component(id = "change-fan-out")
@Consume.FromEventSourcedEntity(DestinationEntity.class)
public class ChangeConsumer extends Consumer {

  private final ComponentClient componentClient;

  public ChangeConsumer(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public Effect onEvent(DestinationEvent event) {
    return switch (event) {
      case DestinationEvent.ChangeAccepted accepted -> {
        componentClient
            .forWorkflow(accepted.change().id())
            .method(FanOutWorkflow::start)
            .invoke(accepted.change());
        yield effects().done();
      }
    };
  }
}
