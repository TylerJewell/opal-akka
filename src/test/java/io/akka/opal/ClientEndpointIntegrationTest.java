package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The client's own routes, over real HTTP — SPEC-002 R91, R97, R98 and R99.
 *
 * <p>The connectivity pair is ordered, because {@code already_disabled} is only reachable from
 * {@code disabled}: a table of independent calls would see {@code disabled} twice and never the
 * answer the second call actually gives.
 *
 * <p>The policy store is the in-memory one so the client has no child process to start; every
 * rule tested here is about the client's own surface rather than about an engine.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(OpalProcessExtension.class)
public class ClientEndpointIntegrationTest extends TestKitSupport {

  static void startProcess() {
    System.setProperty("OPAL_ROLE", "client");
    System.setProperty("OPAL_POLICY_STORE_TYPE", "MOCK");
    System.setProperty("OPAL_INLINE_OPA_ENABLED", "false");
    System.setProperty("OPAL_OFFLINE_MODE_ENABLED", "false");
    System.setProperty("OPAL_DEFAULT_OPAL_SERVER_CONNECTIVITY_DISABLED", "false");
  }

  @AfterAll
  public static void clearProperties() {
    for (String name :
        List.of(
            "OPAL_ROLE",
            "OPAL_POLICY_STORE_TYPE",
            "OPAL_INLINE_OPA_ENABLED",
            "OPAL_OFFLINE_MODE_ENABLED",
            "OPAL_DEFAULT_OPAL_SERVER_CONNECTIVITY_DISABLED")) {
      System.clearProperty(name);
    }
  }

  private static JsonNode recorded(String key) {
    return SourceAnswers.LIVE_CLIENT.get(key);
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  /** R127: the server's routes are not mounted on a client. */
  @Test
  @Order(1)
  public void theServerRoutesAreAbsentOnAClient() {
    assertEquals(404, httpClient.GET("/policy").invoke().status().intValue());
    assertEquals(404, httpClient.GET("/pubsub_client_info").invoke().status().intValue());
    assertEquals(404, httpClient.GET("/statistics").invoke().status().intValue());
  }

  /** R91: ready and healthy answer different questions, and both are unavailable before a load. */
  @Test
  @Order(2)
  public void healthBeforeAnyLoadIsUnavailable() {
    StrictResponse<ByteString> ready = httpClient.GET("/ready").invoke();
    assertEquals(503, ready.status().intValue());
    assertEquals("unavailable", body(ready).get("status").asText());

    StrictResponse<ByteString> healthy = httpClient.GET("/healthy").invoke();
    assertEquals(503, healthy.status().intValue());
  }

  /** The client's own policy-store description, in the shape the source reports it. */
  @Test
  @Order(3)
  public void policyStoreConfigMatchesTheSourcesShape() {
    StrictResponse<ByteString> response = httpClient.GET("/policy-store/config").invoke();
    assertEquals(recorded("policy_store_config").get("status").asInt(), response.status().intValue());
    JsonNode expected = recorded("policy_store_config").get("body");
    JsonNode actual = body(response);
    assertEquals(expected.get("auth_type").asText(), actual.get("auth_type").asText());
    assertNotNull(actual.get("url"));
    // R292: `OPA` whatever store this client has. The field is the schema's own default and the
    // route never fills it in, which is what the recorded answer shows — this run uses the
    // in-memory store and the original said `OPA` for it too.
    assertEquals(
        expected.get("type").asText(),
        actual.get("type").asText(),
        "the field is a constant, not the configured store");
  }

  /** R97: registering, reading back, listing and removing a callback. */
  @Test
  @Order(4)
  public void callbacksMatchTheSource() {
    StrictResponse<ByteString> registered =
        httpClient
            .POST("/callbacks")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"url\": \"http://127.0.0.1:9/cb\", \"config\": {\"method\": \"post\"}}"
                    .getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(recorded("callbacks_register").get("status").asInt(), registered.status().intValue());
    String key = body(registered).get("key").asText();
    assertEquals(recorded("callbacks_register").get("body").get("key").asText(), key);

    assertEquals(
        recorded("callbacks_get").asInt(),
        httpClient.GET("/callbacks/" + key).invoke().status().intValue());

    StrictResponse<ByteString> missing = httpClient.GET("/callbacks/nope").invoke();
    assertEquals(recorded("callbacks_get_missing").get("status").asInt(), missing.status().intValue());
    assertEquals(
        recorded("callbacks_get_missing").get("body").get("detail").asText(),
        body(missing).get("detail").asText());

    StrictResponse<ByteString> listed = httpClient.GET("/callbacks").invoke();
    assertEquals(recorded("callbacks_list").get("status").asInt(), listed.status().intValue());
    assertTrue(body(listed).size() >= 1);

    assertEquals(
        recorded("callbacks_delete").asInt(),
        httpClient.DELETE("/callbacks/" + key).invoke().status().intValue());
    assertEquals(
        recorded("callbacks_delete_missing").asInt(),
        httpClient.DELETE("/callbacks/" + key).invoke().status().intValue());
  }

  /** R99: both toggles refuse unless offline mode is on. */
  @Test
  @Order(5)
  public void connectivityTogglesRefuseWithoutOfflineMode() {
    StrictResponse<ByteString> status = httpClient.GET("/opal-server/connectivity").invoke();
    assertEquals(200, status.status().intValue());
    assertEquals(false, body(status).get("offline_mode_enabled").asBoolean());
    assertEquals(false, body(status).get("opal_server_connectivity_disabled").asBoolean());

    assertEquals(
        400, httpClient.POST("/opal-server/connectivity/disable").invoke().status().intValue());
    assertEquals(
        400, httpClient.POST("/opal-server/connectivity/enable").invoke().status().intValue());
  }

  /** R92: the two trigger routes answer, whether or not a server is reachable. */
  @Test
  @Order(6)
  public void theTriggerRoutesAnswer() {
    StrictResponse<ByteString> policy = httpClient.POST("/policy-updater/trigger").invoke();
    assertEquals(recorded("policy_trigger").get("status").asInt(), policy.status().intValue());
    assertEquals(recorded("policy_trigger").get("body"), body(policy));

    StrictResponse<ByteString> data = httpClient.POST("/data-updater/trigger").invoke();
    assertEquals(recorded("data_trigger").get("status").asInt(), data.status().intValue());
    assertEquals(recorded("data_trigger").get("body"), body(data));
  }

  /** R140: the client renders the same two screens, over its own document. */
  @Test
  @Order(7)
  public void theClientRendersItsOwnDocument() {
    StrictResponse<ByteString> openapi = httpClient.GET("/openapi.json").invoke();
    assertEquals(200, openapi.status().intValue());
    assertEquals(
        recorded("client_openapi_title").asText(),
        body(openapi).get("info").get("title").asText());
    assertEquals(
        SourceAnswers.strings(recorded("client_openapi_paths")),
        io.akka.opal.api.OpenApi.paths(body(openapi)));

    assertEquals(recorded("client_docs_status").asInt(),
        httpClient.GET("/docs").invoke().status().intValue());
    assertEquals(recorded("client_redoc_status").asInt(),
        httpClient.GET("/redoc").invoke().status().intValue());
  }
}
