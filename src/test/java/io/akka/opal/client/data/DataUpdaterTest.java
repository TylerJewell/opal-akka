package io.akka.opal.client.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.store.MockPolicyStoreClient;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.util.Hashing;
import io.akka.opal.server.pubsub.Rpc;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-002 R46 to R53, driven through the in-memory store. */
class DataUpdaterTest {

  private static DataUpdater updater(
      MockPolicyStoreClient store, List<String> topics, boolean splitRoot) {
    DataFetcher fetcher = new DataFetcher(io.akka.opal.common.util.Http.plain(), 5);
    CallbacksRegister register =
        new CallbacksRegister(List.of(), Data.HttpFetcherConfig.defaultCallbackConfig());
    return new DataUpdater(
        store, fetcher, register, new CallbacksReporter(register, fetcher), topics, false,
        splitRoot);
  }

  private static Data.DataSourceEntry inline(
      List<String> topics, String dstPath, String saveMethod, Object data) {
    return new Data.DataSourceEntry(null, null, topics, dstPath, saveMethod, data, null);
  }

  /** R46: an entry whose topics do not meet the client's subscriptions is not applied. */
  @Test
  void onlyMatchingTopicsAreApplied() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), false);
    updater.updatePolicyData(
        new Data.DataUpdate(
            null,
            List.of(
                inline(List.of("policy_data"), "/kept", "PUT", Map.of("a", 1)),
                inline(List.of("other"), "/dropped", "PUT", Map.of("b", 2))),
            "probe",
            null));
    assertEquals(List.of("/kept"), store.writtenPaths());
  }

  /** R46: an entry with no topics at all reaches nobody. */
  @Test
  void anEntryWithNoTopicsIsSkipped() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), false);
    updater.updatePolicyData(
        new Data.DataUpdate(
            null, List.of(inline(List.of(), "/x", "PUT", Map.of("a", 1))), "probe", null));
    assertEquals(List.of(), store.writtenPaths());
  }

  /** R48 and R49: inline data is used as-is, and the destination gains its leading slash. */
  @Test
  void inlineDataIsWrittenAtTheNormalisedPath() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), false);
    updater.updatePolicyData(
        new Data.DataUpdate(
            null,
            List.of(inline(List.of("policy_data"), "users", "PUT", Map.of("a", 1))),
            "probe",
            null));
    assertEquals(List.of("/users"), store.writtenPaths());
    assertEquals(1, store.getData("/users").get("a").asInt());
  }

  /** R50: with the root split on, each top-level key is written under its own path. */
  @Test
  void splittingTheRootWritesEachKeySeparately() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), true);
    updater.updatePolicyData(
        new Data.DataUpdate(
            null,
            List.of(inline(List.of("policy_data"), "", "PUT", Map.of("a", 1, "b", 2))),
            "probe",
            null));
    assertEquals(2, store.writtenPaths().size());
    assertTrue(store.writtenPaths().contains("/a"));
    assertTrue(store.writtenPaths().contains("/b"));
  }

  /** R52: the hash is of the fetched value, and matches the digest the source computes. */
  @Test
  void reportHashesMatchTheSource() throws Exception {
    JsonNode recorded = SourceAnswers.get("calc_hash");
    List<Object> payloads =
        List.of(
            Rpc.MAPPER.readTree("{\"a\": 1}"),
            Rpc.MAPPER.readTree("[1, 2, 3]"),
            "plain string",
            Rpc.MAPPER.readTree("{}"),
            Rpc.MAPPER.readTree("{\"nested\": {\"b\": [1, {\"c\": 2}]}}"));
    for (int i = 0; i < payloads.size(); i++) {
      assertEquals(recorded.get(i).asText(), Hashing.calcHash(payloads.get(i)), "payload " + i);
    }
  }

  /** R53: a fetch that failed has no hash at all. */
  @Test
  void aFailedFetchReportsNoHash() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), false);
    Data.DataSourceEntry entry =
        new Data.DataSourceEntry(
            "http://127.0.0.1:1/never", null, List.of("policy_data"), "/x", "PUT", null, null);
    io.akka.opal.client.store.StoreTransactionContext transaction =
        new io.akka.opal.client.store.StoreTransactionContext(
            store, "t", io.akka.opal.common.schemas.Store.TransactionType.data);
    Data.DataEntryReport report = updater.fetchAndSaveData(entry, transaction);
    assertFalse(report.fetched());
    assertFalse(report.saved());
    assertNull(report.hash());
  }

  /**
   * R51 is about the root; a list written at a named path is left alone, which is the case the
   * rule is not. The wrapping itself belongs to the OPA client and is covered where it happens.
   */
  @Test
  void aListAtANamedPathIsLeftAlone() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), false);
    updater.updatePolicyData(
        new Data.DataUpdate(
            null,
            List.of(inline(List.of("policy_data"), "/items", "PUT", List.of(1, 2, 3))),
            "probe",
            null));
    assertTrue(store.getData("/items").isArray());
  }

  /** R49's PATCH half: the entry's data is applied as an RFC-6902 document. */
  @Test
  void aPatchEntryAppliesItsDocument() {
    MockPolicyStoreClient store = new MockPolicyStoreClient();
    DataUpdater updater = updater(store, List.of("policy_data"), false);
    updater.updatePolicyData(
        new Data.DataUpdate(
            null,
            List.of(inline(List.of("policy_data"), "", "PUT", Map.of("a", Map.of("b", 1)))),
            "probe",
            null));
    updater.updatePolicyData(
        new Data.DataUpdate(
            null,
            List.of(
                inline(
                    List.of("policy_data"),
                    "/",
                    "PATCH",
                    List.of(Map.of("op", "add", "path", "/c", "value", 2)))),
            "probe",
            null));
    assertEquals(2, store.getData("").get("c").asInt());
  }
}
