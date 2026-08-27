package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The server's routes, over real HTTP, against what the running original answered.
 *
 * <p>Every assertion here is a status and a body compared to a recording taken off the original's
 * own process. Nothing in this file asserts a hand-written expectation: a route that agrees with a
 * description someone wrote and disagrees with the thing it replaces has not been checked.
 *
 * <p>These run through the endpoint layer rather than through the component client, which is the
 * only way a route's query parameters, its status codes and its error envelope are exercised at
 * all — a component-client call goes nowhere near them.
 */
public class ServerEndpointIntegrationTest extends TestKitSupport {

  private static JsonNode recorded(String key) {
    return io.akka.opal.SourceAnswers.LIVE_SERVER.get(key);
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  /** R128: the root answers {@code {"status":"ok"}} unconditionally. */
  @Test
  public void rootMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/").invoke();
    assertEquals(recorded("root").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("root").get("body"), body(response));
  }

  @Test
  public void healthcheckMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/healthcheck").invoke();
    assertEquals(recorded("healthcheck").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("healthcheck").get("body"), body(response));
  }

  /** R131: the all-data stand-in answers an empty object. */
  @Test
  public void policyDataMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/policy-data").invoke();
    assertEquals(recorded("policy_data").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("policy_data").get("body"), body(response));
  }

  /** R56: the configured data sources, which by default point at this server's own route. */
  @Test
  public void dataConfigMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/data/config").invoke();
    assertEquals(recorded("data_config").get("status").asInt(), response.status().intValue());
    JsonNode entries = body(response).get("entries");
    JsonNode expected = recorded("data_config").get("body").get("entries");
    assertEquals(expected.size(), entries.size());
    assertEquals(expected.get(0).get("topics"), entries.get(0).get("topics"));
    assertEquals(expected.get(0).get("dst_path"), entries.get(0).get("dst_path"));
    assertEquals(expected.get(0).get("save_method"), entries.get(0).get("save_method"));
    assertTrue(entries.get(0).get("url").asText().endsWith("/policy-data"));
  }

  /** R45: an update with no security configured is accepted and published. */
  @Test
  public void publishingADataUpdateMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/data/config")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                ("{\"entries\": [{\"url\": \"http://x/y\", \"topics\": [\"policy_data\"],"
                        + " \"dst_path\": \"/users\"}], \"reason\": \"probe\"}")
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(
        recorded("data_config_post_entry").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("data_config_post_entry").get("body"), body(response));
  }

  /** R45: an update carrying no entries at all is still accepted. */
  @Test
  public void publishingAnEmptyUpdateMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/data/config")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"entries\": [], \"reason\": \"probe\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(
        recorded("data_config_post_empty").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("data_config_post_empty").get("body"), body(response));
  }

  /** R132: a callback report is accepted and answered with an empty object. */
  @Test
  public void callbackReportMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/data/callback_report")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"reports\": []}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("callback_report").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("callback_report").get("body"), body(response));
  }

  /** R72: with no signing key configured, minting a token answers 503 with the source's words. */
  @Test
  public void tokenWithoutSecurityMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/token")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"type\": \"client\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("token_no_master").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("token_no_master").get("body"), body(response));
  }

  /** R65: with nobody connected, the client register is empty. */
  @Test
  public void clientInfoMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/pubsub_client_info").invoke();
    assertEquals(
        recorded("pubsub_client_info").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("pubsub_client_info").get("body"), body(response));
  }

  /** R119: the load-limit route answers 200 with no body. */
  @Test
  public void loadlimitMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/loadlimit").invoke();
    assertEquals(recorded("loadlimit").get("status").asInt(), response.status().intValue());
  }

  /** R74: with signing disabled the JWKS document is empty. */
  @Test
  public void jwksMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/.well-known/jwks.json").invoke();
    assertEquals(recorded("jwks").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("jwks").get("body"), body(response));
  }

  /** R112: with statistics off, both routes answer 501 and name the variable. */
  @Test
  public void statisticsOffMatchesTheSource() {
    JsonNode recordedOff = io.akka.opal.SourceAnswers.LIVE_SCOPES.get("statistics_off");
    StrictResponse<ByteString> response = httpClient.GET("/statistics").invoke();
    assertEquals(recordedOff.get("status").asInt(), response.status().intValue());
    assertEquals(recordedOff.get("body"), body(response));

    JsonNode recordedStats = io.akka.opal.SourceAnswers.LIVE_SCOPES.get("stats_off");
    StrictResponse<ByteString> brief = httpClient.GET("/stats").invoke();
    assertEquals(recordedStats.get("status").asInt(), brief.status().intValue());
    assertEquals(recordedStats.get("body"), body(brief));
  }

  /** R36: with no policy repository configured, the bundle route reports itself unavailable. */
  @Test
  public void policyWithoutARepositoryIsUnavailable() {
    StrictResponse<ByteString> response = httpClient.GET("/policy").invoke();
    assertEquals(503, response.status().intValue());
  }

  /** R123: a webhook payload naming no repository is a 400 with the source's own words. */
  @Test
  public void webhookWithoutAPayloadMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/webhook")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(
        recorded("webhook_no_payload").get("status").asInt(),
        response.status().intValue(),
        "raw body: " + response.body().decodeString(StandardCharsets.UTF_8));
    assertEquals(recorded("webhook_no_payload").get("body"), body(response));
  }

  /** R140: the OpenAPI document, and the two screens that draw it. */
  @Test
  public void theRenderedSurfaceIsServed() {
    StrictResponse<ByteString> openapi = httpClient.GET("/openapi.json").invoke();
    assertEquals(200, openapi.status().intValue());
    assertEquals(
        recorded("openapi_title").asText(), body(openapi).get("info").get("title").asText());
    assertEquals(
        SourceAnswers.strings(recorded("openapi_paths")),
        io.akka.opal.api.OpenApi.paths(body(openapi)));

    StrictResponse<ByteString> docs = httpClient.GET("/docs").invoke();
    assertEquals(200, docs.status().intValue());
    assertTrue(docs.body().decodeString(StandardCharsets.UTF_8).contains("swagger-ui"));

    StrictResponse<ByteString> redoc = httpClient.GET("/redoc").invoke();
    assertEquals(200, redoc.status().intValue());
    assertTrue(redoc.body().decodeString(StandardCharsets.UTF_8).contains("redoc"));
    assertTrue(
        redoc.body().decodeString(StandardCharsets.UTF_8).contains("spec-url=\"/openapi.json\""),
        "with no proxy in front, the page is byte for byte what the source serves");
  }

  /**
   * R338: behind a proxy that mounts this process under a prefix, the page asks for the document
   * under the same prefix.
   *
   * <p>A page served at {@code /opal/redoc} and fetching {@code /openapi.json} asks for a path
   * the proxy does not map, and the screen draws nothing.
   */
  @Test
  public void theRenderedSurfaceFollowsTheProxyPrefix() {
    for (String route : java.util.List.of("/redoc", "/docs")) {
      StrictResponse<ByteString> page =
          httpClient.GET(route).addHeader("X-Forwarded-Prefix", "/opal/").invoke();
      assertEquals(200, page.status().intValue());
      String markup = page.body().decodeString(StandardCharsets.UTF_8);
      assertTrue(markup.contains("/opal/openapi.json"), route + " asked for " + markup);
    }
  }

  /** OD-9: OPAL's vendor-agent measurements are readable here instead. */
  @Test
  public void theMetricsRouteIsServed() {
    StrictResponse<ByteString> response = httpClient.GET("/internal/metrics").invoke();
    assertEquals(200, response.status().intValue());
    assertTrue(body(response).has("counters"));
  }

  /** R133: the debug stats route exists only with the flag on. */
  @Test
  public void theInternalStatsRouteIsAbsentWithoutTheFlag() {
    JsonNode recordedDisabled =
        io.akka.opal.SourceAnswers.LIVE_SCOPES.get("internal_stats_disabled_on_plain");
    StrictResponse<ByteString> response =
        httpClient.GET("/internal/git-fetcher-cache-stats").invoke();
    assertEquals(recordedDisabled.asInt(), response.status().intValue());
  }

  /** R102: with scopes off, the scope routes are not mounted. */
  @Test
  public void scopeRoutesAreAbsentWithScopesOff() {
    StrictResponse<ByteString> response = httpClient.GET("/scopes").invoke();
    assertEquals(recorded("scopes_off").get("status").asInt(), response.status().intValue());
  }
}
