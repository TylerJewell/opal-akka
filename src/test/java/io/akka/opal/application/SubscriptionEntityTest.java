package io.akka.opal.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.KeyValueEntityTestKit;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 §2 — what one member of the fleet has asked to hear about. */
class SubscriptionEntityTest {

  @Test
  void anUnknownSubscriberWantsNothing() {
    var testKit = KeyValueEntityTestKit.of("pdp-1", SubscriptionEntity::new);
    var result = testKit.method(SubscriptionEntity::read).invoke();
    assertThat(result.getReply().subscriber()).isEqualTo("pdp-1");
    assertThat(result.getReply().topics()).isEmpty();
  }

  @Test
  void subscribingReplacesTheWholeSetRatherThanAddingToIt() {
    var testKit = KeyValueEntityTestKit.of("pdp-1", SubscriptionEntity::new);
    testKit
        .method(SubscriptionEntity::subscribe)
        .invoke(new SubscriptionEntity.Subscribe(List.of("policy_data", "billing")));
    testKit
        .method(SubscriptionEntity::subscribe)
        .invoke(new SubscriptionEntity.Subscribe(List.of("billing")));
    assertThat(testKit.getState().topics()).containsExactly("billing");
  }

  @Test
  void topicsAreStoredAsWrittenAndNotExpanded() {
    // Expansion happens where a change is published, not where a subscription is recorded —
    // that asymmetry is what lets a subscription to a branch match a change to a leaf.
    var testKit = KeyValueEntityTestKit.of("pdp-1", SubscriptionEntity::new);
    testKit
        .method(SubscriptionEntity::subscribe)
        .invoke(new SubscriptionEntity.Subscribe(List.of("policy_data/users/keys")));
    assertThat(testKit.getState().topics()).containsExactly("policy_data/users/keys");
  }
}
