package io.akka.opal.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.opal.application.CursorEntity;
import io.akka.opal.application.DestinationEntity;
import io.akka.opal.application.SubscriptionEntity;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.CursorState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * The claim the whole port is named after, against a running service: a burst of changes to
 * one destination reaches every subscriber addressed by it, in the order they were published,
 * with nothing missing and nothing applied twice.
 *
 * <p>This is also question-log row 27's evidence for the target's own guarantee. Reading the
 * documentation says the runtime keeps the order; only this says it under load.
 */
public class PropagationOrderIntegrationTest extends TestKitSupport {

  private static final ObjectMapper JSON = new ObjectMapper();

  private void subscribe(String subscriber, List<String> topics) {
    componentClient
        .forKeyValueEntity(subscriber)
        .method(SubscriptionEntity::subscribe)
        .invoke(new SubscriptionEntity.Subscribe(topics));
  }

  private Change publish(String destination, List<String> topics, String payload) {
    return componentClient
        .forEventSourcedEntity(destination)
        .method(DestinationEntity::publish)
        .invoke(new DestinationEntity.Publish(topics, payload, "burst", "batch"));
  }

  private CursorState cursor(String subscriber, String destination) {
    return componentClient
        .forEventSourcedEntity(CursorEntity.Id.of(subscriber, destination))
        .method(CursorEntity::read)
        .invoke();
  }

  @Test
  public void aBurstArrivesInPublishOrderAtEverySubscriber() {
    String destination = "/burst";
    List<String> fleet = List.of("pdp-a", "pdp-b", "pdp-c");
    fleet.forEach(s -> subscribe(s, List.of("policy_data")));

    int count = 200;
    List<Long> published = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      published.add(publish(destination, List.of("policy_data"), "{\"v\":" + i + "}").sequence());
    }
    assertThat(published).isSorted();
    assertThat(published.get(count - 1)).isEqualTo((long) count);

    for (String subscriber : fleet) {
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(
              () -> {
                CursorState state = cursor(subscriber, destination);
                assertThat(state.applied()).isEqualTo((long) count);
                assertThat(state.gaps()).isEmpty();
                assertThat(state.value()).contains("{\"v\":" + count + "}");
              });
    }
  }

  @Test
  public void aSubscriberThatMissedTheStartIsCaughtUpByNumber() {
    // Rules 12, 16, 17. The subscriber joins after change 3, so change 4 arrives above the
    // next expected one, is refused, and the gap is closed from the destination's own log.
    String destination = "/late";
    for (int i = 1; i <= 3; i++) {
      publish(destination, List.of("policy_data"), "{\"v\":" + i + "}");
    }
    subscribe("pdp-late", List.of("policy_data"));
    publish(destination, List.of("policy_data"), "{\"v\":4}");

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              CursorState state = cursor("pdp-late", destination);
              assertThat(state.applied()).isEqualTo(4L);
              assertThat(state.gaps()).isEmpty();
              assertThat(state.value()).contains("{\"v\":4}");
            });
  }

  @Test
  public void nothingIsLostWhileASubscriberIsUnreachable() {
    // Rule 20. The subscriber does not exist while four changes are published, which is the
    // case the source answers with a bounded buffer that drops the oldest (question-log
    // row 18). Here the destination is the record, so the whole span is still there.
    String destination = "/absent";
    for (int i = 1; i <= 4; i++) {
      publish(destination, List.of("policy_data"), "{\"v\":" + i + "}");
    }
    subscribe("pdp-absent", List.of("policy_data"));
    publish(destination, List.of("policy_data"), "{\"v\":5}");

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              CursorState state = cursor("pdp-absent", destination);
              assertThat(state.applied()).isEqualTo(5L);
              assertThat(state.value()).contains("{\"v\":5}");
            });
  }

  @Test
  public void aSubscriberIsUntouchedByAChangeItIsNotAddressedTo() {
    // Rule 15.
    String destination = "/scoped";
    subscribe("pdp-elsewhere", List.of("other_topic"));
    subscribe("pdp-here", List.of("policy_data"));
    publish(destination, List.of("policy_data"), "{\"v\":1}");

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(cursor("pdp-here", destination).applied()).isEqualTo(1L));
    assertThat(cursor("pdp-elsewhere", destination).applied()).isEqualTo(0L);
  }

  @Test
  public void aBranchSubscriberIsReachedByALeafChange() {
    // Rules 1 to 3, end to end.
    String destination = "/branch";
    subscribe("pdp-branch", List.of("policy_data"));
    publish(destination, List.of("policy_data/users/keys"), "{\"v\":1}");

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(cursor("pdp-branch", destination).applied()).isEqualTo(1L));
  }

  @Test
  public void convergenceIsAQuestionWithAnAnswer() {
    // Rule 19, and §4 OD-4: the source can only infer this from logs.
    //
    // Its own topic, not the shared one. Every test in this class runs against one service,
    // and a subscriber to a topic is addressed by every change on it whatever the
    // destination — which is the rule working, and which made the first version of this
    // test count seven subscribers at a destination two had been named for.
    String destination = "/converge";
    String topic = "converge_topic";
    subscribe("pdp-1", List.of(topic));
    subscribe("pdp-2", List.of(topic));
    for (int i = 1; i <= 3; i++) {
      publish(destination, List.of(topic), "{\"v\":" + i + "}");
    }

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              var response =
                  httpClient
                      .GET("/propagation/convergence?destination=" + destination)
                      .responseBodyAs(PropagationEndpoint.Convergence.class)
                      .invoke();
              assertThat(response.body().sequence()).isEqualTo(3L);
              assertThat(response.body().subscribers()).hasSize(2);
              assertThat(response.body().converged()).isTrue();
              assertThat(response.body().subscribers())
                  .allSatisfy(s -> assertThat(s.behind()).isEqualTo(0L));
            });
  }

  @Test
  public void changesToTwoDestinationsCarryNoOrderBetweenThem() {
    // Rule 14 stated as something observable: each destination numbers from 1 on its own, so
    // there is no number that orders one against the other.
    subscribe("pdp-two", List.of("policy_data"));
    Change first = publish("/left", List.of("policy_data"), "{\"v\":1}");
    Change second = publish("/right", List.of("policy_data"), "{\"v\":1}");
    assertThat(first.sequence()).isEqualTo(1L);
    assertThat(second.sequence()).isEqualTo(1L);
    assertThat(first.id()).isNotEqualTo(second.id());
  }

  @Test
  public void aPayloadSurvivesUnchanged() throws Exception {
    String destination = "/exact";
    subscribe("pdp-exact", List.of("policy_data"));
    String payload = JSON.readTree("{\"nested\":{\"a\":[1,2,3]},\"b\":null}").toString();
    publish(destination, List.of("policy_data"), payload);

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(cursor("pdp-exact", destination).value()).contains(payload));
  }
}
