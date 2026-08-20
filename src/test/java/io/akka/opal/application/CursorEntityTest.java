package io.akka.opal.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.opal.domain.CursorEvent;
import io.akka.opal.domain.CursorState;
import io.akka.opal.domain.Gap;
import io.akka.opal.domain.Outcome;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 10 to 13 and 17 to 18 — one subscriber's position in one sequence. */
class CursorEntityTest {

  private static EventSourcedTestKit<CursorState, CursorEvent, CursorEntity> kit() {
    return EventSourcedTestKit.of("pdp-1@/users", CursorEntity::new);
  }

  private static CursorEntity.Apply apply(long sequence) {
    return new CursorEntity.Apply(sequence, "{\"v\":" + sequence + "}");
  }

  @Test
  void changesAreAppliedInSequenceOrder() {
    var testKit = kit();
    for (long s = 1; s <= 3; s++) {
      var result = testKit.method(CursorEntity::apply).invoke(apply(s));
      assertThat(result.getReply().outcome()).isEqualTo(Outcome.APPLIED);
    }
    assertThat(testKit.getState().applied()).isEqualTo(3L);
    assertThat(testKit.getState().value()).contains("{\"v\":3}");
  }

  @Test
  void theValueAndThePositionMoveTogether() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    CursorState state = testKit.getState();
    assertThat(state.applied()).isEqualTo(1L);
    assertThat(state.value()).contains("{\"v\":1}");
  }

  @Test
  void aRepeatIsIgnoredAndReportedAsADuplicate() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    var again = testKit.method(CursorEntity::apply).invoke(new CursorEntity.Apply(1, "{\"v\":99}"));
    assertThat(again.getReply().outcome()).isEqualTo(Outcome.DUPLICATE);
    assertThat(testKit.getState().value()).contains("{\"v\":1}");
    assertThat(testKit.getState().applied()).isEqualTo(1L);
  }

  @Test
  void aChangeAheadOfTheNextExpectedOneIsNotApplied() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    var ahead = testKit.method(CursorEntity::apply).invoke(apply(4));
    assertThat(ahead.getReply().outcome()).isEqualTo(Outcome.GAP);
    assertThat(testKit.getState().applied()).isEqualTo(1L);
    assertThat(testKit.getState().value()).contains("{\"v\":1}");
  }

  @Test
  void theRecordedGapIncludesTheChangeThatRevealedIt() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    testKit.method(CursorEntity::apply).invoke(apply(4));
    assertThat(testKit.getState().gaps()).containsExactly(new Gap(2, 4));
  }

  @Test
  void applyingTheMissingSpanClosesTheGap() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    testKit.method(CursorEntity::apply).invoke(apply(4));
    for (long s = 2; s <= 4; s++) {
      testKit.method(CursorEntity::apply).invoke(apply(s));
    }
    assertThat(testKit.getState().applied()).isEqualTo(4L);
    assertThat(testKit.getState().gaps()).isEmpty();
    assertThat(testKit.getState().value()).contains("{\"v\":4}");
  }

  @Test
  void applyingPartOfASpanShrinksTheGapRatherThanClearingIt() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    testKit.method(CursorEntity::apply).invoke(apply(5));
    testKit.method(CursorEntity::apply).invoke(apply(2));
    assertThat(testKit.getState().applied()).isEqualTo(2L);
    assertThat(testKit.getState().gaps()).containsExactly(new Gap(3, 5));
  }

  @Test
  void takingTheCurrentValueOutrightClearsEverythingMissing() {
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    testKit.method(CursorEntity::apply).invoke(apply(9));
    var taken =
        testKit.method(CursorEntity::takeValue).invoke(new CursorEntity.TakeValue(12, "{\"v\":12}"));
    assertThat(taken.getReply().outcome()).isEqualTo(Outcome.APPLIED);
    assertThat(testKit.getState().applied()).isEqualTo(12L);
    assertThat(testKit.getState().gaps()).isEmpty();
    assertThat(testKit.getState().value()).contains("{\"v\":12}");
  }

  @Test
  void aRefusalIsRecordedRatherThanDroppedSilently() {
    // The source drops what it cannot place without trace (question-log rows 11, 16); the
    // decision is what makes convergence answerable afterwards.
    var testKit = kit();
    testKit.method(CursorEntity::apply).invoke(apply(1));
    testKit.method(CursorEntity::apply).invoke(apply(1));
    testKit.method(CursorEntity::apply).invoke(apply(7));
    assertThat(testKit.getAllEvents()).hasSize(3);
  }
}
