package io.akka.opal.client.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.store.PolicyStoreClient;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.util.Repr;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R46 to R53, compared as the trail of writes rather than as the state left behind.
 *
 * <p>The source's own probe recorded, for seven updates, every call its store received in order —
 * which path, which method, which value. Two of the seven leave the store looking identical to a
 * third and got there differently: splitting the root writes two documents where writing the root
 * writes one, and a list at the root arrives unwrapped here and is wrapped by the engine's own
 * client. A comparison of the resulting document cannot tell those apart.
 *
 * <p>The two markers the source's probe put around each transaction are left out of the
 * comparison: they record where its own stand-in opened and closed a context, which is a fact
 * about the probe. What each side actually did — the writes and the per-source outcome — is
 * compared in full.
 */
class DataWriteTrailTest {

  /** A store that remembers what it was asked to do, in order. */
  private static final class TrailStore implements PolicyStoreClient {

    private final List<List<Object>> calls = new ArrayList<>();
    private final boolean failWrite;

    TrailStore(boolean failWrite) {
      this.failWrite = failWrite;
    }

    @Override
    public void setPolicyData(JsonNode policyData, String path, String transactionId) {
      calls.add(List.of("set_policy_data", path, policyData));
      if (failWrite) {
        throw new IllegalStateException("write failed");
      }
    }

    @Override
    public void patchPolicyData(
        List<Store.JSONPatchAction> actions, String path, String transactionId) {
      List<String> printed = new ArrayList<>();
      actions.forEach(action -> printed.add(Repr.python(action)));
      calls.add(List.of("patch_policy_data", path, printed));
      if (failWrite) {
        throw new IllegalStateException("write failed");
      }
    }

    @Override
    public void logTransaction(Store.StoreTransaction transaction) {
      if (transaction.remotes_status() == null) {
        return;
      }
      for (Store.RemoteStatus status : transaction.remotes_status()) {
        calls.add(
            List.of(
                "remote",
                status.remote_url(),
                status.succeed(),
                status.error() == null ? "" : status.error()));
      }
    }

    // Nothing else on the interface is reached by a data update.
    @Override
    public void setPolicy(String policyId, String policyCode, String transactionId) {}

    @Override
    public String getPolicy(String policyId) {
      return null;
    }

    @Override
    public Map<String, String> getPolicies() {
      return Map.of();
    }

    @Override
    public void deletePolicy(String policyId, String transactionId) {}

    @Override
    public List<String> getPolicyModuleIds() {
      return List.of();
    }

    @Override
    public void setPolicies(Policy.PolicyBundle bundle, String transactionId) {}

    @Override
    public String getPolicyVersion() {
      return null;
    }

    @Override
    public void deletePolicyData(String path, String transactionId) {}

    @Override
    public JsonNode getData(String path) {
      return null;
    }

    @Override
    public Proxied getDataWithInput(String path, JsonNode input) {
      return null;
    }

    @Override
    public void initHealthcheckPolicy(String policyId, String policyCode) {}

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public boolean isHealthy() {
      return true;
    }

    @Override
    public String fullExport() {
      return "{}";
    }

    @Override
    public void fullImport(String contents) {}
  }

  /** A fetcher that answers from a table, and can be told to fail for one url. */
  private static final class TableFetcher extends DataFetcher {
    private final Map<String, JsonNode> payloads;
    private final String failing;

    TableFetcher(Map<String, JsonNode> payloads, String failing) {
      super(io.akka.opal.common.util.Http.plain(), 5);
      this.payloads = payloads;
      this.failing = failing;
    }

    @Override
    public JsonNode handleUrl(String url, Map<String, Object> config, Object data) {
      if (data != null) {
        return Rpc.MAPPER.valueToTree(data);
      }
      if (url != null && url.equals(failing)) {
        throw new IllegalStateException("boom");
      }
      JsonNode payload = payloads.get(url);
      return payload == null ? Rpc.MAPPER.createObjectNode().put("fetched", url) : payload;
    }
  }

  private static DataUpdater updater(TrailStore store, DataFetcher fetcher, boolean splitRoot) {
    CallbacksRegister register =
        new CallbacksRegister(List.of(), Data.HttpFetcherConfig.defaultCallbackConfig());
    return new DataUpdater(
        store,
        fetcher,
        register,
        new CallbacksReporter(register, fetcher),
        List.of("policy_data"),
        false,
        splitRoot);
  }

  private static Data.DataUpdate update(Data.DataSourceEntry... entries) {
    return new Data.DataUpdate(null, List.of(entries), "r", null);
  }

  /** The trail as JSON, so a whole sequence compares in one assertion. */
  private static String asJson(List<List<Object>> calls) {
    return Rpc.MAPPER.valueToTree(calls).toString();
  }

  private static String recordedTrail(String name) {
    JsonNode recorded = SourceAnswers.get("data_write").get(name);
    List<JsonNode> kept = new ArrayList<>();
    recorded.forEach(
        call -> {
          String kind = call.get(0).asText();
          if (!kind.equals("begin") && !kind.equals("end")) {
            kept.add(call);
          }
        });
    return Rpc.MAPPER.valueToTree(kept).toString();
  }

  @Test
  void aPutAtANamedPathFollowsTheSourcesTrail() {
    TrailStore store = new TrailStore(false);
    updater(store, new TableFetcher(Map.of("http://a", Rpc.MAPPER.createObjectNode().put("v", 1)),
            null), false)
        .updatePolicyData(
            update(new Data.DataSourceEntry("http://a", null, List.of("policy_data"), "users",
                null, null, null)));
    assertEquals(recordedTrail("put_named_path"), asJson(store.calls));
  }

  @Test
  void aPatchWithInlineDataFollowsTheSourcesTrail() {
    TrailStore store = new TrailStore(false);
    updater(store, new TableFetcher(Map.of(), null), false)
        .updatePolicyData(
            update(
                new Data.DataSourceEntry(
                    "http://a",
                    null,
                    List.of("policy_data"),
                    "/users",
                    "PATCH",
                    List.of(Map.of("op", "add", "path", "/x", "value", 1)),
                    null)));
    assertEquals(recordedTrail("patch_inline"), asJson(store.calls));
  }

  @Test
  void anEntryWhoseTopicsDoNotMatchReachesTheStoreNotAtAll() {
    TrailStore store = new TrailStore(false);
    updater(store, new TableFetcher(Map.of(), null), false)
        .updatePolicyData(
            update(new Data.DataSourceEntry("http://a", null, List.of("other"), null, null, null,
                null)));
    assertEquals(recordedTrail("topic_mismatch"), asJson(store.calls));
  }

  @Test
  void anEntryWithNoTopicsReachesTheStoreNotAtAll() {
    TrailStore store = new TrailStore(false);
    updater(store, new TableFetcher(Map.of(), null), false)
        .updatePolicyData(
            update(new Data.DataSourceEntry("http://a", null, List.of(), null, null, null, null)));
    assertEquals(recordedTrail("no_topics"), asJson(store.calls));
  }

  /** R51's other half: the wrapping happens in the engine's client, not on the way to it. */
  @Test
  void aListAtTheRootArrivesUnwrapped() {
    TrailStore store = new TrailStore(false);
    updater(store,
            new TableFetcher(Map.of("http://a", Rpc.MAPPER.createArrayNode().add(1).add(2)), null),
            false)
        .updatePolicyData(
            update(new Data.DataSourceEntry("http://a", null, List.of("policy_data"), "", null,
                null, null)));
    assertEquals(recordedTrail("list_at_root"), asJson(store.calls));
  }

  /** R50: the root split into one write per top-level key. */
  @Test
  void splittingTheRootFollowsTheSourcesTrail() {
    TrailStore store = new TrailStore(false);
    updater(store,
            new TableFetcher(
                Map.of("http://a", Rpc.MAPPER.createObjectNode().put("k1", 1).put("k2", 2)), null),
            true)
        .updatePolicyData(
            update(new Data.DataSourceEntry("http://a", null, List.of("policy_data"), "/", null,
                null, null)));
    assertEquals(recordedTrail("split_root"), asJson(store.calls));
  }

  /** R53: an update whose first entry fails still applies the second. */
  @Test
  void oneFailingEntryDoesNotStopTheNext() {
    TrailStore store = new TrailStore(false);
    // The source's stand-in answered `{"v": <url>}` for whatever it did not refuse; the value is
    // what the write carries, so both sides have to be fed the same one.
    updater(
            store,
            new TableFetcher(
                Map.of("http://ok", Rpc.MAPPER.createObjectNode().put("v", "http://ok")),
                "http://bad"),
            false)
        .updatePolicyData(
            update(
                new Data.DataSourceEntry("http://bad", null, List.of("policy_data"), "/a", null,
                    null, null),
                new Data.DataSourceEntry("http://ok", null, List.of("policy_data"), "/b", null,
                    null, null)));
    List<List<Object>> withoutRemotes = new ArrayList<>();
    store.calls.forEach(
        call -> {
          if (!"remote".equals(call.get(0))) {
            withoutRemotes.add(call);
          }
        });
    assertEquals(recordedTrail("partial_failure"), asJson(withoutRemotes));
  }
}
