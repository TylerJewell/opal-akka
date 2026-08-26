package io.akka.opal.common.fetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.pubsub.Rpc;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R145 to R151 — the register of named providers and the queue that works them.
 *
 * <p>The framework exists so that a deployment can fetch from something OPAL has never heard of:
 * an entry names a provider, the register is asked for it, and the engine runs it. What is worth
 * checking is the three places that indirection can go wrong — a name nobody registered, a
 * configuration that overrides the caller's choice, and an event that reaches a worker without an
 * identity.
 */
class FetcherEngineTest {

  /** A server that echoes what it was asked, which is what the source's own probe used. */
  private static final class EchoServer implements AutoCloseable {
    private final HttpServer server;

    EchoServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", this::handle);
      server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
      exchange.getRequestBody().readAllBytes();
      String authorization = exchange.getRequestHeaders().getFirst("Authorization");
      String body =
          Rpc.MAPPER
              .createObjectNode()
              .put("method", exchange.getRequestMethod())
              .put("path", exchange.getRequestURI().getPath())
              .put("auth", authorization)
              .toString();
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("content-type", "application/json");
      exchange.sendResponseHeaders(200, bytes.length);
      exchange.getResponseBody().write(bytes);
      exchange.close();
    }

    String url(String path) {
      return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  /** R148 and R183: the register ships the source's two providers, and refuses a third name. */
  @Test
  void theBuiltInProviderSetIsTheSources() {
    JsonNode recorded = SourceAnswers.get("fetcher_engine");
    FetcherRegister register = new FetcherRegister(Http.plain(), 5);
    // Both sides sorted: the recorded answer is, and the order a register lists its providers in
    // is not something either side promises — the source's is whatever its module import yielded.
    List<String> names = new ArrayList<>(register.names());
    java.util.Collections.sort(names);
    assertEquals(SourceAnswers.strings(recorded.get("builtin_names")), names);

    assertEquals(
        "NoMatchingFetchProviderException", recorded.get("unknown_fetcher").asText());
    assertThrows(
        FetcherRegister.NoMatchingFetchProviderException.class,
        () -> register.getFetcher("Nope", new FetchEvent("http://x", "Nope", null, null)));
  }

  /** R148, R150, R151: a registered provider is reached by name, and the event carries an id. */
  @Test
  void aRegisteredProviderIsReachedAndTheEventIsIdentified() throws Exception {
    JsonNode recorded = SourceAnswers.get("fetcher_engine");
    FetcherRegister register = new FetcherRegister(Http.plain(), 5);
    List<String> seen = new ArrayList<>();

    register.registerFetcher(
        "RecordingProvider",
        event ->
            new FetchProvider() {
              @Override
              public Object fetch() {
                seen.add(event.url());
                return event.url();
              }

              @Override
              public JsonNode process(Object raw) {
                return Rpc.MAPPER.getNodeFactory().textNode(String.valueOf(raw));
              }
            });

    assertTrue(register.names().contains(recorded.get("registered").asText()));

    try (FetchingEngine engine = new FetchingEngine(register, 2, 5, 5)) {
      FetchEvent queued =
          engine.queueFetchEvent(
              new FetchEvent(recorded.get("handle_url").asText(), "RecordingProvider", null, null),
              data -> {});
      assertTrue(recorded.get("queued_event_has_id").asBoolean(), "the source's event had one too");
      assertNotNull(queued.id(), "the engine gave the event an id");
      assertEquals(32, queued.id().length());

      JsonNode answer =
          engine.handleUrl(recorded.get("handle_url").asText(), null, "RecordingProvider");
      assertEquals(recorded.get("handle_url").asText(), answer.asText());
      assertTrue(seen.contains(recorded.get("handle_url").asText()));
    }
  }

  /** R150: the configuration's own {@code fetcher} wins over the caller's default. */
  @Test
  void aConfigurationNamingAFetcherOverridesTheDefault() throws Exception {
    JsonNode recorded = SourceAnswers.get("fetcher_engine");
    FetcherRegister register = new FetcherRegister(Http.plain(), 5);
    List<String> used = new ArrayList<>();

    register.registerFetcher(
        recorded.get("queued_event_fetcher").asText(),
        event ->
            new FetchProvider() {
              @Override
              public Object fetch() {
                used.add(event.fetcher());
                return "";
              }

              @Override
              public JsonNode process(Object raw) {
                return Rpc.MAPPER.createObjectNode();
              }
            });

    try (FetchingEngine engine = new FetchingEngine(register, 1, 5, 5)) {
      engine.handleUrl(
          "http://y", Map.of("fetcher", recorded.get("queued_event_fetcher").asText()), null);
    }
    assertEquals(List.of(recorded.get("queued_event_fetcher").asText()), used);
  }

  /** R147, over the five configurations the source's probe ran against a loopback server. */
  @Test
  void theHttpProviderAnswersTheSourcesFiveWays() throws Exception {
    JsonNode recorded = SourceAnswers.get("http_fetch_provider");

    try (EchoServer server = new EchoServer();
        FetchingEngine engine = new FetchingEngine(Http.plain(), 5)) {

      JsonNode plain = engine.handleUrl(server.url("/x"), null, null);
      assertEquals(recorded.get("get_json").get("method").asText(), plain.get("method").asText());
      assertEquals(recorded.get("get_json").get("path").asText(), plain.get("path").asText());
      assertTrue(plain.get("auth").isNull());

      JsonNode posted =
          engine.handleUrl(server.url("/x"), Map.of("method", "post", "data", "{}"), null);
      assertEquals(recorded.get("post").get("method").asText(), posted.get("method").asText());

      JsonNode withHeaders =
          engine.handleUrl(
              server.url("/x"), Map.of("headers", Map.of("Authorization", "Bearer T")), null);
      assertEquals(
          recorded.get("with_headers").get("auth").asText(), withHeaders.get("auth").asText());

      JsonNode raw = engine.handleUrl(server.url("/x"), Map.of("is_json", false), null);
      assertTrue(raw.isTextual(), "a non-json fetch answers with the body as text");
      assertEquals(
          Rpc.MAPPER.readTree(recorded.get("raw_text").asText()), Rpc.MAPPER.readTree(raw.asText()));

      // The source hands back its HTTP library's own response object, which its probe could only
      // record by name. What can be compared is that neither side read the body into a value:
      // this one answers the status and the unread text.
      assertEquals("ClientResponse", recorded.get("no_process").asText());
      JsonNode unprocessed = engine.handleUrl(server.url("/x"), Map.of("process_data", false), null);
      assertEquals(200, unprocessed.get("status").asInt());
      assertTrue(unprocessed.get("text").isTextual(), "the body is carried unread");
    }
  }

  /** A failure on a queued event reaches the registered handlers rather than being swallowed. */
  @Test
  void aFailedFetchReachesTheFailureHandlers() throws Exception {
    FetcherRegister register = new FetcherRegister(Http.plain(), 5);
    register.registerFetcher(
        "Exploding",
        event ->
            new FetchProvider() {
              @Override
              public Object fetch() {
                throw new IllegalStateException("boom");
              }

              @Override
              public JsonNode process(Object raw) {
                return null;
              }
            });

    List<String> failures = new ArrayList<>();
    try (FetchingEngine engine = new FetchingEngine(register, 1, 5, 5)) {
      engine.registerFailureHandler((error, event) -> failures.add(event.url()));
      // R179: a fetch is retried two hundred times, and the wait between attempts doubles. The
      // waiting is replaced here and the attempts are not: this is about where a failure goes
      // once the attempts are exhausted, and at the real spacing that would be next century.
      engine.setSleeper(seconds -> true);
      assertThrows(
          IllegalStateException.class, () -> engine.handleUrl("http://boom", null, "Exploding"));
    }
    assertEquals(List.of("http://boom"), failures);
  }
}
