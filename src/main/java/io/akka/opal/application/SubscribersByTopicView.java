package io.akka.opal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.opal.domain.Subscription;
import java.util.List;

/**
 * Who a change is addressed to — SPEC-001 §3 rule 3, read side.
 *
 * <p>The membership test is against the subscriber's own topics, which are stored
 * unexpanded, and the change arrives with its topics already expanded. That asymmetry is
 * deliberate and is what makes a subscription to a branch match a change to a leaf.
 */
@Component(id = "subscribers-by-topic")
public class SubscribersByTopicView extends View {

  public record Subscribers(List<Subscription> items) {}

  @Consume.FromKeyValueEntity(SubscriptionEntity.class)
  public static class SubscriptionsUpdater extends TableUpdater<Subscription> {
    public Effect<Subscription> onUpdate(Subscription subscription) {
      return effects().updateRow(subscription);
    }
  }

  @Query("SELECT * AS items FROM subscribers_by_topic WHERE :topic = ANY(topics)")
  public QueryEffect<Subscribers> byTopic(String topic) {
    return queryResult();
  }
}
