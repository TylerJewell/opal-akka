package io.akka.opal.client.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.akka.opal.client.callbacks.CallbacksRegister;
import io.akka.opal.client.callbacks.CallbacksReporter;
import io.akka.opal.client.data.DataFetcher;
import io.akka.opal.client.store.StubPolicyStore;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.pubsub.Rpc;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R93 and R94 — which bundle the client asks for, and for which directories.
 *
 * <p>Both are read off the query string the client sends, because that is where the rule lands:
 * an update naming three directories of which this client watches one has to become one
 * {@code path=} parameter, and a store that already holds a version has to become a
 * {@code base_hash=}. A test that only looked at what ended up in the store would pass on a
 * client that fetched the whole repository every time.
 */
class PolicyUpdaterTest {

  private HttpServer server;
  private final List<String> queries = new ArrayList<>();
  private volatile String bundleBody;

  @BeforeEach
  void startServer() throws IOException {
    bundleBody =
        Rpc.MAPPER
            .valueToTree(
                new Policy.PolicyBundle(
                    List.of("a.rego"), "H2", null, List.of(),
                    List.of(new Policy.RegoModule("a.rego", "a", "package a\n")), null))
            .toString();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/policy", this::handle);
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    queries.add(exchange.getRequestURI().getRawQuery() == null
            ? ""
            : exchange.getRequestURI().getRawQuery());
    byte[] bytes = bundleBody.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("content-type", "application/json");
    exchange.sendResponseHeaders(200, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private PolicyUpdater updater(StubPolicyStore store, List<String> directories) {
    DataFetcher fetcher = new DataFetcher(Http.plain(), 5);
    CallbacksRegister register =
        new CallbacksRegister(List.of(), Data.HttpFetcherConfig.defaultCallbackConfig());
    PolicyFetcher policyFetcher =
        new PolicyFetcher(
            "http://127.0.0.1:" + server.getAddress().getPort(), null, "default",
            new ConnRetryOptions(null, 0.0, 2, 0.0));
    return new PolicyUpdater(
        store, policyFetcher, new CallbacksReporter(register, fetcher), directories, false);
  }

  /** R94: the client asks only for the directories it watches and the update named. */
  @Test
  void onlyTheIntersectionOfDirectoriesIsAskedFor() {
    PolicyUpdater updater = updater(new StubPolicyStore(), List.of("envs/prod", "shared"));
    updater.onPolicyUpdateMessage(
        new Policy.PolicyUpdateMessage("H1", "H2", List.of("envs/dev", "envs/prod")));

    assertEquals(1, queries.size());
    assertEquals("path=envs%2Fprod", queries.get(0));
  }

  /** R94: an update naming nothing this client watches still asks — for its whole subscription. */
  @Test
  void anUpdateTouchingNothingWatchedFallsBackToTheWholeSubscription() {
    PolicyUpdater updater = updater(new StubPolicyStore(), List.of("envs/prod"));
    updater.onPolicyUpdateMessage(
        new Policy.PolicyUpdateMessage("H1", "H2", List.of("elsewhere")));

    assertEquals(1, queries.size());
    assertEquals("path=envs%2Fprod", queries.get(0));
  }

  /** R93: with nothing in the store the client asks for a complete bundle. */
  @Test
  void anEmptyStoreAsksForACompleteBundle() {
    PolicyUpdater updater = updater(new StubPolicyStore(), List.of("."));
    updater.triggerUpdatePolicy(null, false);
    assertEquals("path=.", queries.get(0));
    assertTrue(!queries.get(0).contains("base_hash"), queries.get(0));
  }

  /**
   * R93: a store holding a version asks for a differential bundle against it — and a forced full
   * update ignores the stored version even though it is there, which is the pair that makes the
   * rule visible. One of the two alone would pass on a client that never sends {@code base_hash}.
   */
  @Test
  void aStoredVersionBecomesABaseHashUnlessAFullUpdateIsForced() {
    StubPolicyStore store = new StubPolicyStore();
    store.setPolicyVersion("H1");
    PolicyUpdater updater = updater(store, List.of("."));

    updater.triggerUpdatePolicy(null, false);
    assertTrue(queries.get(0).contains("base_hash=H1"), queries.get(0));

    updater.triggerUpdatePolicy(null, true);
    assertTrue(!queries.get(1).contains("base_hash"), queries.get(1));
  }
}
