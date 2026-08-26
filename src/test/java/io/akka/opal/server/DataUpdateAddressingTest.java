package io.akka.opal.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.common.schemas.Data;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R22 — where a data update is addressed, and what the entries look like when they land.
 *
 * <p>Two separate things happen and a rebuild can do one without the other. The publication goes
 * to the union of every entry's expansion, so a client subscribed to an ancestor gets it; and each
 * entry's own {@code topics} field is rewritten to its expansion before it goes on the wire, so
 * the receiver — which matches by string equality against what it subscribed to — finds its own
 * topic in the list rather than having to expand a second time.
 *
 * <p>Doing the first and not the second delivers the update to a client that then decides the
 * entry is not for it. That is a silent no-op: the client reports success and writes nothing.
 */
class DataUpdateAddressingTest {

  private static Data.DataSourceEntry entry(String url, List<String> topics) {
    return new Data.DataSourceEntry(url, null, topics, "/x", null, null, null);
  }

  /** R22: the union of every entry's expansion, in first-seen order and without repeats. */
  @Test
  void thePublicationAddressesTheUnionOfEveryExpansion() {
    ServerRuntime.Addressed addressed =
        ServerRuntime.addressDataUpdate(
            new Data.DataUpdate(
                null,
                List.of(
                    entry("http://a", List.of("policy_data/users/keys")),
                    entry("http://b", List.of("policy_data/roles"))),
                "r",
                null),
            null);

    assertEquals(
        List.of(
            "policy_data",
            "policy_data/users",
            "policy_data/users/keys",
            "policy_data/roles"),
        addressed.topics(),
        "the shared ancestor appears once, where it was first seen");
  }

  /** R22: each entry's own topics are the expansion, so the receiver does not expand again. */
  @Test
  void eachEntryCarriesItsOwnExpansion() {
    ServerRuntime.Addressed addressed =
        ServerRuntime.addressDataUpdate(
            new Data.DataUpdate(
                null, List.of(entry("http://a", List.of("policy_data/users/keys"))), "r", null),
            null);

    assertEquals(
        List.of("policy_data", "policy_data/users", "policy_data/users/keys"),
        addressed.update().entries().get(0).topics(),
        "a client subscribed to the ancestor finds its own topic in the entry");
  }

  /** R22, R109: under a scope every addressed topic is prefixed, and the entries are not. */
  @Test
  void aScopePrefixesTheAddressingAndNotTheEntries() {
    ServerRuntime.Addressed addressed =
        ServerRuntime.addressDataUpdate(
            new Data.DataUpdate(
                null, List.of(entry("http://a", List.of("data:policy_data/users"))), "r", null),
            "tenant1");

    assertEquals(
        List.of("tenant1:data:policy_data", "tenant1:data:policy_data/users"),
        addressed.topics());
    assertEquals(
        List.of("data:policy_data", "data:policy_data/users"),
        addressed.update().entries().get(0).topics(),
        "the entry keeps the unprefixed form the client matches against");
  }

  /** R46, R22: an entry with no topics is left alone and addresses nobody. */
  @Test
  void anEntryWithNoTopicsIsAddressedToNobody() {
    ServerRuntime.Addressed addressed =
        ServerRuntime.addressDataUpdate(
            new Data.DataUpdate(null, List.of(entry("http://a", List.of())), "r", null), null);
    assertEquals(List.of(), addressed.topics());
    assertTrue(addressed.update().entries().get(0).topics().isEmpty());
  }
}
