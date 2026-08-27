package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.common.git.ProbeRepository;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@code GET /policy} against a real repository, compared with what the original served from the
 * same two commits — SPEC-002 R34, R35 and OD-11.
 *
 * <p>The repository is built and the service configured in a static initialiser, because the
 * service starts before any {@code @BeforeAll} of this class runs and it reads its configuration
 * once at start-up, exactly as the original does.
 */
@ExtendWith(OpalProcessExtension.class)
public class BundleEndpointIntegrationTest extends TestKitSupport {

  private static ProbeRepository REPO;
  private static Path CLONE;

  static void startProcess() {
    try {
      REPO = new ProbeRepository();
      CLONE = Files.createTempDirectory("opal-bundle-clone-");
      Files.delete(CLONE);
      System.setProperty("OPAL_ROLE", "server");
      System.setProperty("OPAL_POLICY_REPO_URL", REPO.root.toUri().toString());
      System.setProperty("OPAL_POLICY_REPO_MAIN_BRANCH", "master");
      System.setProperty("OPAL_POLICY_REPO_CLONE_PATH", CLONE.toString());
      System.setProperty("OPAL_POLICY_REPO_WEBHOOK_SECRET", "mysecret");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterAll
  public static void tearDownRepo() throws Exception {
    REPO.close();
    System.clearProperty("OPAL_ROLE");
    System.clearProperty("OPAL_POLICY_REPO_URL");
    System.clearProperty("OPAL_POLICY_REPO_MAIN_BRANCH");
    System.clearProperty("OPAL_POLICY_REPO_CLONE_PATH");
    System.clearProperty("OPAL_POLICY_REPO_WEBHOOK_SECRET");
  }

  /**
   * Waits for the first clone before asserting on what the route answers.
   *
   * <p>{@code 503 policy repo was not found} is the right answer while the watcher's first clone
   * is still running, and every rule here is about what the route answers once it is there. A
   * test that begins asserting the moment the runtime is up is timing the clone.
   */
  @org.junit.jupiter.api.BeforeEach
  public void waitForTheFirstClone() {
    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      if (httpClient.GET("/policy").invoke().status().intValue() == 200) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    throw new IllegalStateException("the policy repository was never cloned");
  }

  private static JsonNode recorded(String key) {
    return SourceAnswers.LIVE_SERVER.get(key);
  }

  private JsonNode body(StrictResponse<ByteString> response) {
    try {
      return Rpc.MAPPER.readTree(response.body().decodeString(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(
          "not json: " + response.body().decodeString(StandardCharsets.UTF_8), e);
    }
  }

  /** The two commit hashes are redacted on both sides, as the source's own probe did. */
  private JsonNode redacted(JsonNode bundle) {
    ObjectNode node = (ObjectNode) bundle.deepCopy();
    for (String field : java.util.List.of("hash", "old_hash")) {
      JsonNode value = node.get(field);
      if (value != null && !value.isNull()) {
        if (value.asText().equals(REPO.first.getName())) {
          node.put(field, "H1");
        } else if (value.asText().equals(REPO.second.getName())) {
          node.put(field, "H2");
        }
      }
    }
    return node;
  }

  /** R34: the whole repository at the head commit. */
  @Test
  public void theFullBundleMatchesTheSource() {
    StrictResponse<ByteString> response = httpClient.GET("/policy").invoke();
    assertEquals(recorded("policy_full").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("policy_full").get("body"), redacted(body(response)));
  }

  /** R34: named paths, and a leading slash on one of them, return only those directories. */
  @Test
  public void aScopedBundleMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient.GET("/policy").addQueryParameter("path", "envs").invoke();
    assertEquals(
        recorded("policy_path_envs").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("policy_path_envs").get("body"), redacted(body(response)));

    StrictResponse<ByteString> withSlash =
        httpClient.GET("/policy").addQueryParameter("path", "/envs").invoke();
    assertEquals(
        recorded("policy_path_slash").get("status").asInt(), withSlash.status().intValue());
    assertEquals(recorded("policy_path_slash").get("body"), redacted(body(withSlash)));
  }

  /** R34: a path the repository does not hold is a 404 naming it. */
  @Test
  public void anUnknownPathMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient.GET("/policy").addQueryParameter("path", "nope").invoke();
    assertEquals(
        recorded("policy_path_missing").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("policy_path_missing").get("body"), body(response));
  }

  /** R34: a base hash the repository holds returns the difference between the two commits. */
  @Test
  public void aDifferentialBundleMatchesTheSource() {
    StrictResponse<ByteString> response =
        httpClient
            .GET("/policy")
            .addQueryParameter("base_hash", REPO.first.getName())
            .invoke();
    assertEquals(recorded("policy_diff").get("status").asInt(), response.status().intValue());
    assertEquals(recorded("policy_diff").get("body"), redacted(body(response)));
  }

  /**
   * OD-11: a base hash the repository does not hold. The original answers 500 by two routes; this
   * answers the complete bundle, which is the only answer that lets a client that has lost its
   * place recover.
   */
  @Test
  public void anUnknownBaseHashAnswersTheFullBundle() {
    JsonNode sourceAnswer = recorded("policy_unknown_base");
    assertEquals(500, sourceAnswer.get("status").asInt(), "what the original does");

    StrictResponse<ByteString> response =
        httpClient
            .GET("/policy")
            .addQueryParameter("base_hash", "0".repeat(40))
            .invoke();
    assertEquals(200, response.status().intValue());
    assertEquals(recorded("policy_full").get("body"), redacted(body(response)));

    StrictResponse<ByteString> malformed =
        httpClient.GET("/policy").addQueryParameter("base_hash", "not-a-hash").invoke();
    assertEquals(200, malformed.status().intValue());
  }

  /** R44 and R120 to R122: a signed webhook naming the tracked repository is accepted. */
  @Test
  public void aSignedWebhookMatchesTheSource() {
    String payload =
        "{\"ref\": \"refs/heads/master\", \"repository\": {\"clone_url\": \""
            + REPO.root.toUri()
            + "\"}}";
    String signature =
        io.akka.opal.common.util.Hashing.hex(
            io.akka.opal.common.util.Aws.hmac(
                "mysecret".getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8)));

    StrictResponse<ByteString> response =
        httpClient
            .POST("/webhook")
            .addHeader("x-hub-signature-256", "sha256=" + signature)
            .addHeader("X-GitHub-Event", "push")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                payload.getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(200, response.status().intValue());
    assertEquals("ok", body(response).get("status").asText());
    assertEquals("push", body(response).get("event").asText());
  }

  /** R120: a webhook with a wrong signature is refused. */
  @Test
  public void anUnsignedWebhookIsRefused() {
    StrictResponse<ByteString> response =
        httpClient
            .POST("/webhook")
            .addHeader("x-hub-signature-256", "sha256=deadbeef")
            .withRequestBody(
                akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
                "{\"repository\": {\"clone_url\": \"x\"}}".getBytes(StandardCharsets.UTF_8))
            .invoke();
    assertEquals(401, response.status().intValue());
    assertTrue(body(response).get("detail").asText().contains("signatures"));
  }
}
