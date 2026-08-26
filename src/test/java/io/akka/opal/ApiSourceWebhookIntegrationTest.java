package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import akka.http.javadsl.model.ContentTypes;
import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R126 — the webhook a bundle-server deployment gets instead of the git one.
 *
 * <p>Two things change together and only one of them is obvious. The credential becomes a JWT
 * rather than a git provider's shared secret, which is the visible half; the other is that the
 * route stops reading the payload at all — a bundle server has no repository to name, so there is
 * nothing to match against and every call triggers the check. A rebuild that kept the matching
 * would answer {@code ignored} to every webhook such a deployment sends.
 */
public class ApiSourceWebhookIntegrationTest extends TestKitSupport {

  static {
    System.setProperty("OPAL_ROLE", "server");
    System.setProperty("OPAL_POLICY_SOURCE_TYPE", "API");
    System.setProperty("OPAL_POLICY_BUNDLE_URL", "http://127.0.0.1:1/bundles");
    System.setProperty("OPAL_POLICY_REPO_URL", "");
  }

  @AfterAll
  public static void clearProperties() {
    for (String name :
        List.of(
            "OPAL_ROLE",
            "OPAL_POLICY_SOURCE_TYPE",
            "OPAL_POLICY_BUNDLE_URL",
            "OPAL_POLICY_REPO_URL")) {
      System.clearProperty(name);
    }
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  /** R126: an empty body triggers, because there is no repository to match against. */
  @Test
  public void anyCallTriggersAndNamesTheBundleUrl() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/webhook")
            .withRequestBody(ContentTypes.APPLICATION_JSON, "{}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(200, response.status().intValue());
    assertEquals("ok", body(response).get("status").asText());
    assertEquals("webhook_trigger", body(response).get("event").asText());
    assertEquals("http://127.0.0.1:1/bundles", body(response).get("repo_url").asText());
  }

  /**
   * R126: a payload that would be refused under a git source — no repository anywhere in it — is
   * accepted here. Under a git source the same body is a 400.
   */
  @Test
  public void aPayloadNamingNoRepositoryIsAcceptedUnderABundleSource() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/webhook")
            .addHeader("x-github-event", "push")
            .withRequestBody(
                ContentTypes.APPLICATION_JSON,
                "{\"zen\": \"hello\"}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(200, response.status().intValue());
    assertEquals("webhook_trigger", body(response).get("event").asText());
  }
}
