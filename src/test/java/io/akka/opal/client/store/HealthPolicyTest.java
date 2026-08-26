package io.akka.opal.client.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.schemas.Store;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R90 — the transaction log as a Rego module the engine itself holds.
 *
 * <p>Compared as the whole rendered text rather than field by field, because what a Rego module
 * means depends on its exact syntax: a boolean written {@code True} instead of {@code true}, or a
 * timestamp quoted differently, is a module that does not compile, and the engine reports that by
 * refusing the write rather than by answering a different value.
 */
class HealthPolicyTest {

  /** The template the source's own probe rendered, character for character. */
  private static final String TEMPLATE =
      "package system.opal.transactions\n\n"
          + "default ready = false\nready = {ready}\n"
          + "default healthy = false\nhealthy = {healthy}\n"
          + "last_policy_transaction = {last_policy_transaction}\n"
          + "last_data_transaction = {last_data_transaction}\n"
          + "last_failed_policy_transaction = {last_failed_policy_transaction}\n"
          + "last_failed_data_transaction = {last_failed_data_transaction}\n"
          + "transaction_data_statistics = {transaction_data_statistics}\n"
          + "transaction_policy_statistics = {transaction_policy_statistics}\n";

  @Test
  void theRenderedModuleIsTheSourcesOwn() throws Exception {
    JsonNode recorded = SourceAnswers.get("health_policy_render");

    TransactionLogState state = new TransactionLogState(true, true);
    state.processTransaction(
        new Store.StoreTransaction(
            "1",
            List.of("set_policies"),
            Store.TransactionType.policy,
            true,
            "",
            "t0",
            "t1",
            null));

    try (RecordingEngine engine = new RecordingEngine(java.util.Map.of())) {
      OpaClient client =
          new OpaClient(
              engine.url(), null, io.akka.opal.common.config.Enums.PolicyStoreAuth.NONE, null,
              null, null, true, true, false, null, null, null, List.of(),
              "engine/healthcheck/opal.rego",
              io.akka.opal.common.config.Options.ConnRetryOptions.defaults());
      TransactionLogPolicyWriter writer =
          new TransactionLogPolicyWriter(client, recorded.get("id").asText(), TEMPLATE);
      writer.persist(state);

      assertEquals(
          List.of(List.of("set_policy", recorded.get("id").asText())), engine.calls());
      assertEquals(recorded.get("code").asText(), engine.policies().get(recorded.get("id").asText()));
    }
  }
}
