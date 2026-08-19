package io.akka.opal.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.Delivery;
import io.akka.opal.domain.Entry;
import io.akka.opal.domain.MemberEvent;
import io.akka.opal.domain.MemberState;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SPEC-001 rules 8-14 and 19: what one member does with each change it is handed. */
class MemberEntityTest {

  private static final Instant AT = Instant.parse("2026-08-19T12:00:00Z");

  private static Change change(long position, String address, String destination, String value) {
    return new Change(
        "change-" + position,
        "policy_data",
        position,
        "test",
        List.of(new Entry(address, destination, value)),
        AT);
  }

  private static EventSourcedTestKit<MemberState, MemberEvent, MemberEntity> joined(
      String... watching) {
    var kit = EventSourcedTestKit.of("member-under-test", MemberEntity::new);
    kit.method(MemberEntity::join)
        .invoke(new MemberEntity.Join("policy_data", Set.of(watching)));
    return kit;
  }

  @Test
  void appliesTheFirstChangeAndMovesToPositionOne() {
    var kit = joined("policy_data");
    var result = kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));

    assertEquals(Delivery.Outcome.APPLIED, result.getReply().outcome());
    assertEquals(1L, result.getReply().position());
    assertEquals("one", kit.getState().store().get("/a"));
    assertEquals(1L, kit.getState().applied());
  }

  @Test
  void appliesChangesInPositionOrder() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::deliver).invoke(change(2, "policy_data", "/a", "two"));
    kit.method(MemberEntity::deliver).invoke(change(3, "policy_data", "/a", "three"));

    assertEquals("three", kit.getState().store().get("/a"));
    assertEquals(3L, kit.getState().position());
  }

  @Test
  void ignoresAChangeItHasAlreadyApplied() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::deliver).invoke(change(2, "policy_data", "/a", "two"));

    var again = kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));

    assertEquals(Delivery.Outcome.DUPLICATE, again.getReply().outcome());
    // Rule 9 forbids a repeat changing the store, and rule 19 asks for the count of them
    // to be readable, so the count is written down and the store is not touched.
    assertEquals(
        MemberEvent.DuplicateSeen.class, again.getAllEvents().get(0).getClass());
    assertEquals("two", kit.getState().store().get("/a"));
    assertEquals(2L, kit.getState().position());
    assertEquals(1L, kit.getState().duplicates());
  }

  @Test
  void ignoresTheChangeItIsSittingOnWhenItArrivesAgain() {
    // The common repeat: the fleet hands over the change this member just applied. It is
    // separate from the test above because "behind" and "exactly here" are different
    // comparisons, and a rule that catches only the first passes that one.
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    var again = kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));

    assertEquals(Delivery.Outcome.DUPLICATE, again.getReply().outcome());
    assertEquals(1L, kit.getState().position());
    assertEquals(1L, kit.getState().applied());
    assertEquals(1L, kit.getState().duplicates());
  }

  @Test
  void refusesAChangeMoreThanOnePastItAndStaysPut() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));

    var ahead = kit.method(MemberEntity::deliver).invoke(change(3, "policy_data", "/a", "three"));

    assertEquals(Delivery.Outcome.GAP, ahead.getReply().outcome());
    assertEquals(1L, kit.getState().position());
    assertEquals("one", kit.getState().store().get("/a"));
    assertEquals(1L, kit.getState().gaps());
  }

  @Test
  void closesAGapWhenTheMissingChangeArrives() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::deliver).invoke(change(3, "policy_data", "/a", "three"));
    kit.method(MemberEntity::deliver).invoke(change(2, "policy_data", "/a", "two"));
    var closed = kit.method(MemberEntity::deliver).invoke(change(3, "policy_data", "/a", "three"));

    assertEquals(Delivery.Outcome.APPLIED, closed.getReply().outcome());
    assertEquals(3L, kit.getState().position());
    assertEquals("three", kit.getState().store().get("/a"));
  }

  @Test
  void skipsAChangeAddressedSomewhereItIsNotWatching() {
    var kit = joined("policy_data/users");
    var result =
        kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));

    assertEquals(Delivery.Outcome.NOT_WATCHED, result.getReply().outcome());
    assertTrue(kit.getState().store().isEmpty());
  }

  @Test
  void aSkippedChangeStillMovesThePositionSoTheNextOneIsNotAGap() {
    var kit = joined("policy_data/users");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    var next =
        kit.method(MemberEntity::deliver).invoke(change(2, "policy_data/users", "/b", "two"));

    assertEquals(Delivery.Outcome.APPLIED, next.getReply().outcome());
    assertEquals(2L, kit.getState().position());
    assertEquals("two", kit.getState().store().get("/b"));
  }

  @Test
  void appliesEveryEntryOfOneChangeInTheOrderGiven() {
    var kit = joined("policy_data");
    var many =
        new Change(
            "many",
            "policy_data",
            1,
            "test",
            List.of(
                new Entry("policy_data", "/a", "first"),
                new Entry("policy_data", "/b", "second"),
                new Entry("policy_data", "/a", "last")),
            AT);
    kit.method(MemberEntity::deliver).invoke(many);

    assertEquals("last", kit.getState().store().get("/a"));
    assertEquals("second", kit.getState().store().get("/b"));
    assertEquals(1L, kit.getState().position());
  }

  @Test
  void aChangeToTheRootReplacesEverythingUnderIt() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::deliver).invoke(change(2, "policy_data", "/b", "two"));
    kit.method(MemberEntity::deliver).invoke(change(3, "policy_data", "/", "wiped"));

    assertEquals(1, kit.getState().store().size());
    assertEquals("wiped", kit.getState().store().get("/"));
  }

  @Test
  void aChangeToAParentReplacesWhatIsUnderItAndNothingElse() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a/x", "under a"));
    kit.method(MemberEntity::deliver).invoke(change(2, "policy_data", "/ab", "beside a"));
    kit.method(MemberEntity::deliver).invoke(change(3, "policy_data", "/a", "replaces a"));

    assertEquals("replaces a", kit.getState().store().get("/a"));
    assertEquals("beside a", kit.getState().store().get("/ab"));
    assertFalse(kit.getState().store().containsKey("/a/x"));
  }

  @Test
  void reportsWhereItHasGotTo() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::deliver).invoke(change(4, "policy_data", "/a", "four"));

    var report = kit.method(MemberEntity::report).invoke().getReply();
    assertEquals(1L, report.position());
    assertEquals(1L, report.applied());
    assertEquals(1L, report.duplicates());
    assertEquals(1L, report.gaps());
    assertEquals("policy_data", report.channel());
  }

  @Test
  void aMemberThatHasNotJoinedRefusesEveryChange() {
    var kit =
        EventSourcedTestKit.<MemberState, MemberEvent, MemberEntity>of(
            "member-under-test", MemberEntity::new);
    var result = kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    assertTrue(result.isError());
  }

  @Test
  void joiningTwiceDoesNotResetWhatItHasApplied() {
    var kit = joined("policy_data");
    kit.method(MemberEntity::deliver).invoke(change(1, "policy_data", "/a", "one"));
    kit.method(MemberEntity::join).invoke(new MemberEntity.Join("policy_data", Set.of("policy_data")));

    assertEquals(1L, kit.getState().position());
    assertEquals("one", kit.getState().store().get("/a"));
  }
}
