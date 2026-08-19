package io.akka.opal.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * SPEC-001 rules 1-4. The expansion table and the reach table are the same ones the original
 * was measured on in {@code opal-port/probes/probe_02_topic_fanout.py}, so a disagreement here
 * is a disagreement with a measurement rather than with an opinion.
 */
class AddressTest {

  @Test
  void expandsToItselfAndEveryNameAbove() {
    assertEquals(List.of("policy_data"), Address.of("policy_data").expand());
    assertEquals(
        List.of("policy_data", "policy_data/users"), Address.of("policy_data/users").expand());
    assertEquals(
        List.of("policy_data", "policy_data/users", "policy_data/users/keys"),
        Address.of("policy_data/users/keys").expand());
  }

  @Test
  void keepsThePrefixOnEveryExpansion() {
    assertEquals(
        List.of("data:policy_data", "data:policy_data/users", "data:policy_data/users/keys"),
        Address.of("data:policy_data/users/keys").expand());
  }

  @Test
  void onlyTheRightMostColonSeparatesThePrefix() {
    assertEquals(List.of("a:b:c", "a:b:c/d"), Address.of("a:b:c/d").expand());
    // The one that tells the two rules apart: a slash between the two colons. Splitting
    // on the first would put "b/c:d" through the segment split and yield three names.
    assertEquals(List.of("a:b/c:d", "a:b/c:d/e"), Address.of("a:b/c:d/e").expand());
    assertEquals(List.of("x/y:z"), Address.of("x/y:z").expand());
    assertEquals(List.of("a:b", "a:b/c"), Address.of("a:b/c").expand());
  }

  @Test
  void aColonWithNothingBeforeItIsNotAPrefix() {
    assertEquals(List.of("lead"), Address.of(":lead").expand());
    assertEquals(List.of("trail:"), Address.of("trail:").expand());
    assertEquals(List.of("::x"), Address.of("::x").expand());
  }

  @Test
  void handlesTheEdgesTheOriginalWasMeasuredOn() {
    assertEquals(List.of(""), Address.of("").expand());
    assertEquals(List.of("", "/leading"), Address.of("/leading").expand());
    assertEquals(List.of("trailing", "trailing/"), Address.of("trailing/").expand());
  }

  @ParameterizedTest(name = "a change on {0} reaches a member watching {1}: {2}")
  @CsvSource({
    "policy_data/users/keys, policy_data,           true",
    "policy_data/users/keys, policy_data/users,     true",
    "policy_data/users/keys, policy_data/users/keys,true",
    "policy_data,            policy_data/users,     false",
    "policy_data/users,      policy_data/userstore, false",
    "policy_dataX,           policy_data,           false",
  })
  void reachesUpwardsAndNeverDownwards(String published, String watched, boolean reached) {
    assertEquals(reached, Address.of(published).reaches(Set.of(watched)));
  }

  @Test
  void reachesNobodyWhenNobodyIsWatching() {
    assertFalse(Address.of("policy_data").reaches(Set.of()));
  }

  @Test
  void theChannelIsTheFirstNameInTheExpansion() {
    assertEquals("policy_data", Address.of("policy_data/users/keys").channel());
    assertEquals("policy_data", Address.of("policy_data").channel());
    assertEquals("data:policy_data", Address.of("data:policy_data/users/keys").channel());
    assertEquals("a:b:c", Address.of("a:b:c/d").channel());
  }

  @Test
  void refusesAnAddressThatIsNotThere() {
    assertThrows(IllegalArgumentException.class, () -> Address.of(null));
  }

  @Test
  void expansionIsStableWhenAskedTwice() {
    var address = Address.of("policy_data/users/keys");
    assertEquals(address.expand(), address.expand());
    assertTrue(address.expand().contains(address.channel()));
  }
}
