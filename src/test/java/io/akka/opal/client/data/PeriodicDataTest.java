package io.akka.opal.client.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.store.StubPolicyStore;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.pubsub.Rpc;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R54 and R55 — which entries the base load applies, and which become their own timers.
 *
 * <p>The two halves of R55 are separable and only one of them is obvious. An entry carrying a
 * polling interval being left out of the initial load is visible in one call; the previous set of
 * timers being cancelled on the next call is visible only across two, and a client that skipped
 * that would double its polling rate on every reconnect.
 */
class PeriodicDataTest {

  /** A store that remembers where each write landed, in order. */
  private static final class WriteLog extends StubPolicyStore {
    private final List<String> paths = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void setPolicyData(JsonNode policyData, String path, String transactionId) {
      paths.add(path);
    }
  }

  /** A fetcher that answers without a network, which is what the source's probe stood in for. */
  private static DataFetcher offlineFetcher(List<String> reportedTo) {
    return new DataFetcher(Http.plain(), 5) {
      @Override
      public JsonNode handleUrl(String url, Map<String, Object> config, Object inlineData) {
        return Rpc.MAPPER.createObjectNode().put("url", url);
      }

      @Override
      public JsonNode fetch(String url, Data.HttpFetcherConfig config) {
        reportedTo.add(url);
        return Rpc.MAPPER.createObjectNode();
      }
    };
  }

  private static DataUpdater updater(
      StubPolicyStore store, DataFetcher fetcher, List<Object> callbacks, boolean shouldReport) {
    CallbacksRegister register =
        new CallbacksRegister(callbacks, Data.HttpFetcherConfig.defaultCallbackConfig());
    return new DataUpdater(
        store,
        fetcher,
        register,
        new CallbacksReporter(register, fetcher),
        List.of("policy_data"),
        shouldReport,
        false);
  }

  private static Data.DataSourceEntry entry(String name, Double interval) {
    return new Data.DataSourceEntry(
        "http://" + name, null, List.of("policy_data"), "/" + name, null, null, interval);
  }

  /** R55: the initial load carries only the entry with no interval, and the other one fires later. */
  @Test
  void thePeriodicEntryIsLeftOutOfTheInitialLoadAndFiresOnItsOwn() throws Exception {
    JsonNode recorded = SourceAnswers.get("base_data_config");
    WriteLog store = new WriteLog();
    DataUpdater updater = updater(store, offlineFetcher(new ArrayList<>()), List.of(), false);

    updater.getBasePolicyData(
        new Data.DataSourceConfig(List.of(entry("one", null), entry("poll", 0.05))),
        recorded.get("initial").get("reason").asText());

    assertEquals(List.of("/one"), List.copyOf(store.paths), "the initial load, on its own");
    assertEquals(
        List.of("http://one"),
        SourceAnswers.strings(recorded.get("initial").get("urls")),
        "the source's initial load carried the same one entry");

    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!store.paths.contains("/poll") && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    updater.stopPollingUpdateTasks();

    assertTrue(
        recorded.get("periodic_fired_at_least_once").asBoolean(), "it fired for the source too");
    assertTrue(store.paths.contains("/poll"), "and here: " + store.paths);
    assertEquals(
        SourceAnswers.strings(recorded.get("periodic_reasons")),
        List.of(DataUpdater.PERIODIC_UPDATE_REASON),
        "under the reason the source gives it");
  }

  /** R55: a second base load cancels the first one's timers rather than adding to them. */
  @Test
  void aSecondBaseLoadReplacesThePreviousTimers() throws Exception {
    WriteLog store = new WriteLog();
    DataUpdater updater = updater(store, offlineFetcher(new ArrayList<>()), List.of(), false);

    Data.DataSourceConfig config = new Data.DataSourceConfig(List.of(entry("poll", 0.05)));
    updater.getBasePolicyData(config, "Initial load");
    Thread.sleep(150);
    updater.getBasePolicyData(config, "Reconnect");

    store.paths.clear();
    Thread.sleep(400);
    updater.stopPollingUpdateTasks();

    long firings = store.paths.stream().filter("/poll"::equals).count();
    // One timer at 50 ms over 400 ms is around eight firings; two timers would be around sixteen.
    assertTrue(firings >= 1, "the surviving timer still fires, got " + firings);
    assertTrue(firings <= 12, "one timer's worth of firings, got " + firings);
  }

  /** R54: reports go out only when the client was asked to send them. */
  @Test
  void reportsAreSentOnlyWhenAskedFor() {
    for (boolean shouldReport : List.of(false, true)) {
      List<String> reportedTo = Collections.synchronizedList(new ArrayList<>());
      DataUpdater updater =
          updater(
              new StubPolicyStore(),
              offlineFetcher(reportedTo),
              List.of("http://callback"),
              shouldReport);
      updater.updatePolicyData(
          new Data.DataUpdate("u1", List.of(entry("a", null)), "r", null));
      assertEquals(
          shouldReport ? List.of("http://callback") : List.of(),
          List.copyOf(reportedTo),
          "shouldSendReports=" + shouldReport);
    }
  }
}
