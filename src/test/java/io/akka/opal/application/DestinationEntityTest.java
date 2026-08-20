package io.akka.opal.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.DestinationEvent;
import io.akka.opal.domain.DestinationState;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 4, 5, 6, 9, 16, 18 — the destination as the authority on order. */
class DestinationEntityTest {

  private static EventSourcedTestKit<DestinationState, DestinationEvent, DestinationEntity> kit(
      String destination) {
    return EventSourcedTestKit.of(
        destination, context -> new DestinationEntity(context, change -> {}));
  }

  private static DestinationEntity.Publish publish(String payload) {
    return new DestinationEntity.Publish(List.of("policy_data"), payload, "test", "batch-1");
  }

  @Test
  void theFirstChangeIsNumberOne() {
    var testKit = kit("/users");
    var result = testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":1}"));
    assertThat(result.isReply()).isTrue();
    assertThat(result.getReply().sequence()).isEqualTo(1L);
    assertThat(result.getReply().id()).isEqualTo("/users#1");
  }

  @Test
  void sequenceNumbersAreGaplessAndNeverRepeat() {
    var testKit = kit("/users");
    List<Long> assigned =
        List.of(
            testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":1}")).getReply()
                .sequence(),
            testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":2}")).getReply()
                .sequence(),
            testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":3}")).getReply()
                .sequence());
    assertThat(assigned).containsExactly(1L, 2L, 3L);
  }

  @Test
  void theIdentityIsAssignedHereAndReturnedToThePublisher() {
    var testKit = kit("/roles");
    testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":1}"));
    var second = testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":2}"));
    assertThat(second.getReply().id()).isEqualTo("/roles#2");
    assertThat(second.getReply().destination()).isEqualTo("/roles");
  }

  @Test
  void aChangeAddressedToNoTopicIsRefused() {
    var testKit = kit("/users");
    var result =
        testKit
            .method(DestinationEntity::publish)
            .invoke(new DestinationEntity.Publish(List.of(), "{\"a\":1}", "test", "batch-1"));
    assertThat(result.isError()).isTrue();
    assertThat(testKit.getState().sequence()).isEqualTo(0L);
  }

  @Test
  void aChangeWithNoPayloadIsRefused() {
    var testKit = kit("/users");
    var result =
        testKit
            .method(DestinationEntity::publish)
            .invoke(new DestinationEntity.Publish(List.of("policy_data"), null, "t", "b"));
    assertThat(result.isError()).isTrue();
  }

  @Test
  void aPayloadMayBeABareNumberOrString() {
    // Rule 9. The source refuses both: its inline payload must be a document or a list
    // (question-log row 13).
    var testKit = kit("/count");
    assertThat(testKit.method(DestinationEntity::publish).invoke(publish("7")).isReply()).isTrue();
    assertThat(testKit.method(DestinationEntity::publish).invoke(publish("\"seven\"")).isReply())
        .isTrue();
    assertThat(testKit.getState().value()).contains("\"seven\"");
  }

  @Test
  void topicsAreExpandedWhenTheChangeIsAccepted() {
    var testKit = kit("/users");
    var result =
        testKit
            .method(DestinationEntity::publish)
            .invoke(
                new DestinationEntity.Publish(
                    List.of("policy_data/users/keys"), "{\"a\":1}", "t", "b"));
    assertThat(result.getReply().topics())
        .containsExactly("policy_data", "policy_data/users", "policy_data/users/keys");
  }

  @Test
  void theSpanAfterASequenceIsInOrder() {
    var testKit = kit("/users");
    for (int i = 1; i <= 5; i++) {
      testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":" + i + "}"));
    }
    var span = testKit.method(DestinationEntity::since).invoke(2L);
    assertThat(span.getReply().complete()).isTrue();
    assertThat(span.getReply().changes().stream().map(Change::sequence))
        .containsExactly(3L, 4L, 5L);
    assertThat(span.getReply().sequence()).isEqualTo(5L);
  }

  @Test
  void askingFromTheFrontGivesNothingAndIsStillComplete() {
    var testKit = kit("/users");
    testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":1}"));
    var span = testKit.method(DestinationEntity::since).invoke(1L);
    assertThat(span.getReply().complete()).isTrue();
    assertThat(span.getReply().changes()).isEmpty();
  }

  @Test
  void aPayloadOverTheLimitIsRefused() {
    // The retained log is bounded by bytes as well as by count, so a change that could not
    // be retained even on its own is refused rather than accepted and immediately dropped.
    var testKit = kit("/huge");
    String oversized = "{\"a\":\"" + "x".repeat(DestinationState.MAX_PAYLOAD_BYTES) + "\"}";
    var result = testKit.method(DestinationEntity::publish).invoke(publish(oversized));
    assertThat(result.isError()).isTrue();
    assertThat(testKit.getState().sequence()).isEqualTo(0L);
  }

  @Test
  void aSpanReachingBackFurtherThanTheLogIsIncompleteAndOffersTheValueInstead() {
    var testKit = kit("/big");
    for (int i = 1; i <= DestinationState.RETAINED + 5; i++) {
      testKit.method(DestinationEntity::publish).invoke(publish("{\"a\":" + i + "}"));
    }
    var span = testKit.method(DestinationEntity::since).invoke(0L);
    assertThat(span.getReply().complete()).isFalse();
    assertThat(span.getReply().sequence()).isEqualTo((long) DestinationState.RETAINED + 5);
    assertThat(span.getReply().value())
        .contains("{\"a\":" + (DestinationState.RETAINED + 5) + "}");
  }
}
