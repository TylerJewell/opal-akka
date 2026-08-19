package io.akka.opal.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.NotificationPublisher;
import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.opal.domain.ChannelEvent;
import io.akka.opal.domain.ChannelState;
import io.akka.opal.domain.Entry;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** SPEC-001 rules 5-7 and 15-16: giving a change a position, and handing back what came after one. */
class ChannelEntityTest {

  /** The live hint is not what the guarantee rests on, so this probe throws it away. */
  private static final NotificationPublisher<Long> UNHEARD =
      new NotificationPublisher<Long>() {
        @Override
        public void publish(Long position) {}
      };

  private static EventSourcedTestKit<ChannelState, ChannelEvent, ChannelEntity> channel() {
    return EventSourcedTestKit.of("policy_data", ctx -> new ChannelEntity(ctx, UNHEARD));
  }

  private static ChannelEntity.Publish publish(String address, String destination, String value) {
    return new ChannelEntity.Publish(
        null, "test", List.of(new Entry(address, destination, value)));
  }

  @Test
  void theFirstChangeGetsPositionOne() {
    var accepted = channel().method(ChannelEntity::publish).invoke(publish("policy_data", "/a", "1"));
    assertEquals(1L, accepted.getReply().position());
    assertEquals("policy_data", accepted.getReply().channel());
  }

  @Test
  void positionsGoUpByOneAndAreNeverReused() {
    var kit = channel();
    var positions =
        IntStream.rangeClosed(1, 50)
            .mapToObj(n -> kit.method(ChannelEntity::publish)
                .invoke(publish("policy_data", "/a", String.valueOf(n)))
                .getReply()
                .position())
            .toList();

    assertEquals(IntStream.rangeClosed(1, 50).mapToObj(Long::valueOf).toList(), positions);
    assertEquals(50, positions.stream().distinct().count());
  }

  @Test
  void refusesAChangeWhoseAddressesSpanTwoChannels() {
    var kit = channel();
    var across =
        new ChannelEntity.Publish(
            null,
            "test",
            List.of(
                new Entry("policy_data/users", "/a", "1"),
                new Entry("other_root/users", "/b", "2")));

    var result = kit.method(ChannelEntity::publish).invoke(across);
    assertTrue(result.isError());
    assertTrue(result.getError().contains("policy_data"));
    assertTrue(result.getError().contains("other_root"));
  }

  @Test
  void refusesAChangeAddressedToADifferentChannelThanItsOwn() {
    var kit = channel();
    var elsewhere = publish("other_root/users", "/a", "1");
    assertTrue(kit.method(ChannelEntity::publish).invoke(elsewhere).isError());
  }

  @Test
  void refusesAChangeWithNoEntries() {
    var kit = channel();
    var empty = new ChannelEntity.Publish(null, "test", List.of());
    assertTrue(kit.method(ChannelEntity::publish).invoke(empty).isError());
  }

  @Test
  void handsBackEveryChangeAfterAPositionInPositionOrder() {
    var kit = channel();
    IntStream.rangeClosed(1, 5)
        .forEach(n -> kit.method(ChannelEntity::publish)
            .invoke(publish("policy_data", "/a", String.valueOf(n))));

    var since = kit.method(ChannelEntity::changesSince).invoke(2L).getReply();

    assertEquals(List.of(3L, 4L, 5L), since.changes().stream().map(c -> c.position()).toList());
    assertTrue(since.complete());
  }

  @Test
  void handsBackNothingWhenTheCallerIsAlreadyCurrent() {
    var kit = channel();
    kit.method(ChannelEntity::publish).invoke(publish("policy_data", "/a", "1"));

    var since = kit.method(ChannelEntity::changesSince).invoke(1L).getReply();
    assertTrue(since.changes().isEmpty());
    assertTrue(since.complete());
  }

  @Test
  void saysSoRatherThanReturningAPartialListWhenTheCallerIsTooFarBehind() {
    var kit = channel();
    var beyond = ChannelEntity.RETENTION + 5;
    IntStream.rangeClosed(1, beyond)
        .forEach(n -> kit.method(ChannelEntity::publish)
            .invoke(publish("policy_data", "/a", String.valueOf(n))));

    var since = kit.method(ChannelEntity::changesSince).invoke(0L).getReply();

    assertFalse(since.complete());
    assertTrue(since.changes().isEmpty());
    assertEquals(6L, since.earliestRetained());
  }

  @Test
  void keepsOnlyTheMostRecentChanges() {
    var kit = channel();
    var beyond = ChannelEntity.RETENTION + 5;
    IntStream.rangeClosed(1, beyond)
        .forEach(n -> kit.method(ChannelEntity::publish)
            .invoke(publish("policy_data", "/a", String.valueOf(n))));

    assertEquals(ChannelEntity.RETENTION, kit.getState().retained().size());
    var since = kit.method(ChannelEntity::changesSince).invoke(5L).getReply();
    assertTrue(since.complete());
    assertEquals(ChannelEntity.RETENTION, since.changes().size());
  }

  @Test
  void refusesAChangeLargerThanAChannelWillHold() {
    var kit = channel();
    var huge = "x".repeat(ChannelEntity.LARGEST_CHANGE_BYTES + 1);
    var result =
        kit.method(ChannelEntity::publish).invoke(publish("policy_data", "/a", huge));
    assertTrue(result.isError());
  }

  @Test
  void whatIsKeptStaysSmallEnoughToBeCopiedWhole() {
    var kit = channel();
    var chunky = "y".repeat(8 * 1024);
    // Enough of them that the count bound alone would leave far more than fits.
    IntStream.rangeClosed(1, 200)
        .forEach(n -> kit.method(ChannelEntity::publish)
            .invoke(publish("policy_data", "/a", chunky + n)));

    var kept = kit.getState().retained();
    assertTrue(kept.size() < ChannelEntity.RETENTION);
    assertTrue(ChannelState.weigh(kept) <= ChannelEntity.RETAINED_BYTES);
    // Still the newest ones, and still contiguous.
    assertEquals(200L, kept.get(kept.size() - 1).position());
    for (var i = 1; i < kept.size(); i++) {
      assertEquals(kept.get(i - 1).position() + 1, kept.get(i).position());
    }
  }

  @Test
  void refusesAnEntryWithNoAddress() {
    var kit = channel();
    var nameless =
        new ChannelEntity.Publish(null, "test", java.util.Arrays.asList((Entry) null));
    assertTrue(kit.method(ChannelEntity::publish).invoke(nameless).isError());
  }

  @Test
  void aChangeCarriesTheIdItWasGiven() {
    var kit = channel();
    var named =
        new ChannelEntity.Publish(
            "my-own-id", "test", List.of(new Entry("policy_data", "/a", "1")));
    assertEquals("my-own-id", kit.method(ChannelEntity::publish).invoke(named).getReply().id());
  }

  @Test
  void aChangeWithNoIdIsGivenOne() {
    var kit = channel();
    var id = kit.method(ChannelEntity::publish).invoke(publish("policy_data", "/a", "1"))
        .getReply().id();
    assertFalse(id == null || id.isBlank());
  }
}
