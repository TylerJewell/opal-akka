package io.akka.opal.server.webhook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.config.Options.GitWebhookRequestParams;
import io.akka.opal.common.config.Options.SecretTypeEnum;
import io.akka.opal.common.util.Aws;
import io.akka.opal.common.util.Hashing;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-002 R120 to R123, against the signature and the matches the source computed. */
class WebhookTest {

  private static final String GITHUB_PAYLOAD =
      """
      {"ref": "refs/heads/main",
       "repository": {"git_url": "git://github.com/o/r.git",
                      "ssh_url": "git@github.com:o/r.git",
                      "clone_url": "https://github.com/o/r.git",
                      "full_name": "o/r"}}
      """;

  private static final String GITLAB_PAYLOAD =
      """
      {"ref": "refs/heads/main",
       "project": {"git_http_url": "https://gitlab.com/o/r.git",
                   "git_ssh_url": "git@gitlab.com:o/r.git",
                   "path_with_namespace": "o/r"}}
      """;

  private static final String AZURE_PAYLOAD =
      """
      {"refUpdates": {"name": "refs/heads/main"},
       "resource": {"repository": {"remoteUrl": "https://dev.azure.com/o/r"}}}
      """;

  private static final String BARE_REF_PAYLOAD =
      """
      {"ref": "refs/heads/main", "repository": {"url": "https://x/y"}}
      """;

  /** R120: the signature over the raw body, parsed out of the header by the configured regex. */
  @Test
  void signatureValidationMatchesTheSource() {
    byte[] body = "{\"ref\":\"refs/heads/main\"}".getBytes(StandardCharsets.UTF_8);
    String secret = "mysecret";
    String expected = SourceAnswers.get("webhook_signature").asText();
    String computed = Hashing.hex(Aws.hmac(secret.getBytes(StandardCharsets.UTF_8), body));
    assertEquals(expected, computed);

    Webhooks.validateGitSecret(
        secret, GitWebhookRequestParams.github(), "sha256=" + computed, body);
  }

  @Test
  void aWrongSignatureIsRefused() {
    byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
    Webhooks.Refused refused =
        assertThrows(
            Webhooks.Refused.class,
            () ->
                Webhooks.validateGitSecret(
                    "s3cr3t", GitWebhookRequestParams.github(), "sha256=deadbeef", body));
    assertEquals(401, refused.status);
    assertEquals("signatures didn't match!", refused.getMessage());
  }

  @Test
  void aMissingSecretIsRefused() {
    byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
    Webhooks.Refused refused =
        assertThrows(
            Webhooks.Refused.class,
            () -> Webhooks.validateGitSecret("s3cr3t", GitWebhookRequestParams.github(), null, body));
    assertEquals("No secret was provided!", refused.getMessage());
  }

  /** R120: with no secret configured the route is open. */
  @Test
  void withNoSecretConfiguredNothingIsChecked() {
    Webhooks.validateGitSecret(null, GitWebhookRequestParams.github(), null, new byte[0]);
  }

  /** R120's token half: byte equality rather than an HMAC. */
  @Test
  void aTokenSecretIsComparedDirectly() {
    GitWebhookRequestParams params =
        new GitWebhookRequestParams(
            "x-gitlab-token", SecretTypeEnum.token, "(.*)", "X-Gitlab-Event", null, "Push Hook",
            true);
    Webhooks.validateGitSecret("s3cr3t", params, "s3cr3t", new byte[0]);
    assertEquals(
        "secret-tokens didn't match!",
        assertThrows(
                Webhooks.Refused.class,
                () -> Webhooks.validateGitSecret("s3cr3t", params, "other", new byte[0]))
            .getMessage());
  }

  /** R122, over the seven match cases the source's own comparison answered. */
  @Test
  void urlMatchingMatchesTheSource() {
    for (JsonNode row : SourceAnswers.get("is_matching_webhook_url")) {
      String url = row.get("url").asText();
      List<String> urls = SourceAnswers.strings(row.get("urls"));
      List<String> names = SourceAnswers.strings(row.get("names"));
      assertEquals(
          row.get("output").asBoolean(),
          Webhooks.isMatchingWebhookUrl(url, urls, names),
          url + " against " + urls + " / " + names);
    }
  }

  /**
   * R122, R123: the five payloads the source's own probe read, compared field for field.
   *
   * <p>Each provider writes the repository somewhere different, and one of the five names nothing
   * at all - the class is enumerated rather than sampled, because the reading is a fall-through
   * over nine fields and a sample of two says nothing about the seventh.
   */
  @Test
  void everyProviderShapeMatchesTheSource() throws Exception {
    JsonNode shapes = SourceAnswers.get("webhook_secrets").get("payload_shapes");
    Map<String, String> payloads = new LinkedHashMap<>();
    payloads.put("github", GITHUB_PAYLOAD);
    payloads.put("gitlab", GITLAB_PAYLOAD);
    payloads.put("azure", AZURE_PAYLOAD);
    payloads.put("bare_ref", BARE_REF_PAYLOAD);
    payloads.put("empty", "{}");

    for (Map.Entry<String, String> entry : payloads.entrySet()) {
      JsonNode expected = shapes.get(entry.getKey());
      JsonNode payload = Rpc.MAPPER.readTree(entry.getValue());
      if (expected.isTextual()) {
        Webhooks.Refused refused =
            assertThrows(Webhooks.Refused.class, () -> Webhooks.extractGitChanges(payload));
        assertEquals(
            expected.asText(),
            "HTTPException:" + refused.status + ":" + refused.getMessage(),
            entry.getKey());
        continue;
      }
      Webhooks.GitChanges changes = Webhooks.extractGitChanges(payload);
      assertEquals(expected.get("branch").asText(), changes.branch(), entry.getKey());
      assertEquals(
          SourceAnswers.strings(expected.get("urls")), sorted(changes.urls()), entry.getKey());
      assertEquals(
          SourceAnswers.strings(expected.get("names")), sorted(changes.names()), entry.getKey());
    }
  }

  private static List<String> sorted(List<String> values) {
    List<String> out = new ArrayList<>(values);
    java.util.Collections.sort(out);
    return out;
  }

  /** R123: a payload naming neither a URL nor a name is a 400. */
  @Test
  void aPayloadWithNoRepositoryIsRefused() throws Exception {
    Webhooks.Refused refused =
        assertThrows(
            Webhooks.Refused.class,
            () -> Webhooks.extractGitChanges(Rpc.MAPPER.readTree("{\"zen\": \"hello\"}")));
    assertEquals(400, refused.status);
    assertEquals("repo url or full name not found in payload!", refused.getMessage());
  }

  /**
   * OD-6: a payload carrying every one of the nine URL fields is handled rather than answering
   * 500. The source removes one null from a set it has just built, and a payload with no nulls in
   * it raises there.
   */
  @Test
  void aPayloadCarryingEveryUrlFieldIsHandled() throws Exception {
    JsonNode payload =
        Rpc.MAPPER.readTree(
            "{\"repository\": {\"git_url\": \"a\", \"ssh_url\": \"b\", \"clone_url\": \"c\","
                + " \"git_ssh_url\": \"d\", \"git_http_url\": \"e\", \"url\": \"f\","
                + " \"full_name\": \"o/r\"}, \"project\": {\"git_http_url\": \"g\","
                + " \"git_ssh_url\": \"h\", \"path_with_namespace\": \"p/q\"},"
                + " \"resource\": {\"repository\": {\"remoteUrl\": \"i\"}}}");
    Webhooks.GitChanges changes = Webhooks.extractGitChanges(payload);
    assertEquals(9, changes.urls().size());
    assertEquals(2, changes.names().size());
  }
}
