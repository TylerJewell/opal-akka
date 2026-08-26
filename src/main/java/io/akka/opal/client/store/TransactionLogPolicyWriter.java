package io.akka.opal.client.store;

import io.akka.opal.common.util.PythonJson;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the transaction log into the engine as a generated Rego module — SPEC-002 R90.
 *
 * <p>The point is that an authorization query can ask the engine whether OPAL is healthy, without
 * asking OPAL: {@code data.system.opal.ready} is a document the engine itself holds. So the state
 * is rendered into the template's placeholders as JSON and written like any other policy.
 */
public final class TransactionLogPolicyWriter {

  private static final Logger log = LoggerFactory.getLogger(TransactionLogPolicyWriter.class);

  private final PolicyStoreClient store;
  private final String policyId;
  private final String policyTemplate;

  public TransactionLogPolicyWriter(
      PolicyStoreClient store, String policyId, String policyTemplate) {
    this.store = store;
    this.policyId = policyId;
    this.policyTemplate = policyTemplate;
  }

  public void persist(TransactionLogState state) {
    log.info(
        "persisting health check policy: ready={}, healthy={}", state.ready(), state.healthy());
    log.info(
        "Policy and data statistics: policy: (successful {}, failed {});\tdata: (successful {},"
            + " failed {})",
        state.transactionPolicyStatistics().get("successful"),
        state.transactionPolicyStatistics().get("failed"),
        state.transactionDataStatistics().get("successful"),
        state.transactionDataStatistics().get("failed"));

    Map<String, Object> values = new LinkedHashMap<>();
    values.put("ready", state.ready());
    values.put("healthy", state.healthy());
    values.put("last_policy_transaction", state.lastPolicyTransaction());
    values.put("last_failed_policy_transaction", state.lastFailedPolicyTransaction());
    values.put("last_data_transaction", state.lastDataTransaction());
    values.put("last_failed_data_transaction", state.lastFailedDataTransaction());
    values.put("transaction_data_statistics", state.transactionDataStatistics());
    values.put("transaction_policy_statistics", state.transactionPolicyStatistics());

    store.setPolicy(policyId, render(policyTemplate, values), null);
  }

  /** Every {@code {name}} in the template becomes that value written as JSON. */
  static String render(String template, Map<String, Object> values) {
    String out = template;
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      out = out.replace("{" + entry.getKey() + "}", PythonJson.dumps(entry.getValue()));
    }
    return out;
  }
}
