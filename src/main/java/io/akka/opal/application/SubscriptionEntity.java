package io.akka.opal.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import akka.javasdk.keyvalueentity.KeyValueEntityContext;
import io.akka.opal.domain.Subscription;
import java.util.List;

/**
 * What one subscriber wants to hear about. Held as state rather than as a log: only the
 * current set of topics decides delivery, and no rule in SPEC-001 asks how it got that way.
 */
@Component(id = "subscription")
public class SubscriptionEntity extends KeyValueEntity<Subscription> {

  private final String subscriber;

  public SubscriptionEntity(KeyValueEntityContext context) {
    this.subscriber = context.entityId();
  }

  public record Subscribe(List<String> topics) {}

  @Override
  public Subscription emptyState() {
    return new Subscription(subscriber, List.of());
  }

  public Effect<Done> subscribe(Subscribe command) {
    return effects()
        .updateState(new Subscription(subscriber, List.copyOf(command.topics())))
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<Subscription> read() {
    return effects().reply(currentState());
  }
}
