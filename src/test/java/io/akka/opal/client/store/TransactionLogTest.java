package io.akka.opal.client.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.schemas.Store;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R87 to R89, against the five sequences and the reachability trail the source's own
 * state machine produced.
 *
 * <p>Sequences rather than single transactions, because the question is what the state becomes
 * next: {@code ok_then_fail} and {@code fail_then_ok} agree on every count and disagree on health,
 * and neither is visible from one transaction.
 */
class TransactionLogTest {

  private static Store.StoreTransaction tx(Store.TransactionType kind, boolean ok, String id) {
    return new Store.StoreTransaction(
        id, List.of("set_policies"), kind, ok, null, null, null, List.of());
  }

  private static final Map<String, List<Store.StoreTransaction>> SEQUENCES =
      Map.of(
          "policy_ok_then_data_ok",
              List.of(
                  tx(Store.TransactionType.policy, true, "1"),
                  tx(Store.TransactionType.data, true, "2")),
          "policy_ok_only", List.of(tx(Store.TransactionType.policy, true, "1")),
          "policy_fail", List.of(tx(Store.TransactionType.policy, false, "1")),
          "ok_then_fail",
              List.of(
                  tx(Store.TransactionType.policy, true, "1"),
                  tx(Store.TransactionType.data, true, "2"),
                  tx(Store.TransactionType.data, false, "3")),
          "fail_then_ok",
              List.of(
                  tx(Store.TransactionType.data, false, "1"),
                  tx(Store.TransactionType.policy, true, "2"),
                  tx(Store.TransactionType.data, true, "3")));

  @Test
  void everySequenceMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("transaction_log_sequences");
    for (Map.Entry<String, List<Store.StoreTransaction>> sequence : SEQUENCES.entrySet()) {
      TransactionLogState state = new TransactionLogState(true, true);
      JsonNode steps = recorded.get(sequence.getKey());

      assertEquals(steps.get(0).get("ready").asBoolean(), state.ready(),
          sequence.getKey() + " ready at init");
      assertEquals(steps.get(0).get("healthy").asBoolean(), state.healthy(),
          sequence.getKey() + " healthy at init");

      int index = 1;
      for (Store.StoreTransaction transaction : sequence.getValue()) {
        state.processTransaction(transaction);
        JsonNode step = steps.get(index++);
        String where = sequence.getKey() + " after " + transaction.id();
        assertEquals(step.get("ready").asBoolean(), state.ready(), where + " ready");
        assertEquals(step.get("healthy").asBoolean(), state.healthy(), where + " healthy");
        assertEquals(
            step.get("policy_stats").get("successful").asInt(),
            state.transactionPolicyStatistics().get("successful"),
            where + " policy successful");
        assertEquals(
            step.get("policy_stats").get("failed").asInt(),
            state.transactionPolicyStatistics().get("failed"),
            where + " policy failed");
        assertEquals(
            step.get("data_stats").get("successful").asInt(),
            state.transactionDataStatistics().get("successful"),
            where + " data successful");
        assertEquals(
            step.get("data_stats").get("failed").asInt(),
            state.transactionDataStatistics().get("failed"),
            where + " data failed");
      }
    }
  }

  /**
   * R87 and R88: a disabled updater is not waited for. Each variant is run after one successful
   * policy transaction, which is the state the source's own probe measured them in — with none
   * at all every variant answers the same thing and the flags decide nothing.
   */
  @Test
  void disabledUpdatersMatchTheSource() {
    JsonNode recorded = SourceAnswers.get("transaction_log_updater_flags");
    for (boolean data : List.of(true, false)) {
      for (boolean policy : List.of(true, false)) {
        String key = "data=" + python(data) + ",policy=" + python(policy);
        TransactionLogState state = new TransactionLogState(data, policy);
        state.processTransaction(tx(Store.TransactionType.policy, true, "1"));
        assertEquals(recorded.get(key).get("ready").asBoolean(), state.ready(), key + " ready");
        assertEquals(
            recorded.get(key).get("healthy").asBoolean(), state.healthy(), key + " healthy");
      }
    }
  }

  private static String python(boolean value) {
    return value ? "True" : "False";
  }

  /** R89: the reachability flag flips health on its own and flips it back. */
  @Test
  void engineReachabilityMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("transaction_log_engine_reachable");
    TransactionLogState state = new TransactionLogState(true, true);
    assertEquals(recorded.get(0).get("healthy").asBoolean(), state.healthy(), "init");

    state.processTransaction(tx(Store.TransactionType.policy, true, "1"));
    state.processTransaction(tx(Store.TransactionType.data, true, "2"));
    assertEquals(recorded.get(1).get("healthy").asBoolean(), state.healthy(), "two_ok");

    state.setEngineReachable(false);
    assertEquals(recorded.get(2).get("healthy").asBoolean(), state.healthy(), "engine_down");

    state.setEngineReachable(true);
    assertEquals(recorded.get(3).get("healthy").asBoolean(), state.healthy(), "engine_up");
  }
}
