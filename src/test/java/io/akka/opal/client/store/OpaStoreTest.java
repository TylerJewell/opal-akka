package io.akka.opal.client.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.config.Enums.PolicyStoreAuth;
import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R57, R76 to R78, R84 and R85 — what the client writes into OPA, in what order.
 *
 * <p>The order is the rule, not the outcome: a complete bundle and a differential one both end
 * with the same modules in the store, and they get there by different routes. A store that
 * deleted before it wrote would pass any check that only looked at the result and would break a
 * live engine for the length of one bundle.
 */
class OpaStoreTest {

  private static final String HEALTH_PATH = "engine/healthcheck/opal.rego";

  private static OpaClient clientFor(String url, PolicyStoreAuth auth, String token) {
    return new OpaClient(
        url, token, auth, null, null, null, true, true, false, null, null, null, List.of(),
        HEALTH_PATH, new ConnRetryOptions(null, 0.0, 3, 0.0));
  }

  private static OpaClient clientFor(String url) {
    return clientFor(url, PolicyStoreAuth.NONE, null);
  }

  private static List<List<String>> recordedCalls(JsonNode node) {
    List<List<String>> out = new ArrayList<>();
    node.forEach(call -> out.add(List.of(call.get(0).asText(), call.get(1).asText())));
    return out;
  }

  /** The two bundles the source's own probe wrote, rebuilt here so both sides see one input. */
  private static Policy.PolicyBundle completeBundle() {
    return new Policy.PolicyBundle(
        List.of("b.rego", "a.rego", "d/data.json"),
        "H2",
        null,
        List.of(new Policy.DataModule("d", "{\"x\":1}")),
        List.of(
            new Policy.RegoModule("a.rego", "a", "package a\n"),
            new Policy.RegoModule("b.rego", "b", "package b\n")),
        null);
  }

  private static Policy.PolicyBundle deltaBundle() {
    return new Policy.PolicyBundle(
        List.of("b.rego"),
        "H3",
        "H2",
        List.of(new Policy.DataModule("e", "{\"y\":2}")),
        List.of(new Policy.RegoModule("b.rego", "b", "package b2\n")),
        new Policy.DeletedFiles(List.of("gone"), List.of("a.rego")));
  }

  /** R76: data, then policies in manifest order, then a delete of what the bundle omits. */
  @Test
  void aCompleteBundleIsWrittenInTheSourcesOrder() throws Exception {
    JsonNode recorded = SourceAnswers.get("policy_store_order");
    try (RecordingEngine engine = new RecordingEngine(Map.of("stale.rego", "package stale\n"))) {
      OpaClient client = clientFor(engine.url());
      client.setPolicies(completeBundle(), null);

      assertEquals(recordedCalls(recorded.get("complete_bundle_calls")), engine.calls());
      assertEquals(recorded.get("complete_bundle_version").asText(), client.getPolicyVersion());
    }
  }

  /** R77: data writes, data deletes, then policy writes and policy deletes in one round. */
  @Test
  void aDifferentialBundleIsWrittenInTheSourcesOrder() throws Exception {
    JsonNode recorded = SourceAnswers.get("policy_store_order");
    try (RecordingEngine engine = new RecordingEngine(Map.of("a.rego", "package a\n"))) {
      OpaClient client = clientFor(engine.url());
      client.setPolicies(deltaBundle(), null);

      assertEquals(recordedCalls(recorded.get("delta_bundle_calls")), engine.calls());
      assertEquals(recorded.get("delta_bundle_version").asText(), client.getPolicyVersion());
    }
  }

  /**
   * R78, against the attempt sequence the source recorded rather than against a transcription of
   * it: two operations whose first only succeeds once the second has run go {@code b, a, b}.
   */
  @Test
  void theRetryOrderIsTheSourcesOwn() {
    JsonNode recorded = SourceAnswers.get("postponed_retry");
    List<String> attempts = new ArrayList<>();
    List<String> written = new ArrayList<>();

    OpaClient.Operation b =
        () -> {
          attempts.add("b");
          if (!written.contains("a")) {
            return new OpaClient.Answer(400, "");
          }
          written.add("b");
          return new OpaClient.Answer(200, "");
        };
    OpaClient.Operation a =
        () -> {
          attempts.add("a");
          written.add("a");
          return new OpaClient.Answer(200, "");
        };

    OpaClient.attemptOperationsWithPostponedFailureRetry(List.of(b, a));
    assertEquals(SourceAnswers.strings(recorded.get("reordered").get("attempts")), attempts);
    List<String> sorted = new ArrayList<>(written);
    java.util.Collections.sort(sorted);
    assertEquals(SourceAnswers.strings(recorded.get("reordered").get("written")), sorted);
  }

  /** R78's other end: a round in which every operation failed is where it gives up, and says so. */
  @Test
  void givingUpUsesTheSourcesMessage() {
    JsonNode recorded = SourceAnswers.get("postponed_retry");
    List<String> attempts = new ArrayList<>();
    IllegalStateException raised =
        assertThrows(
            IllegalStateException.class,
            () ->
                OpaClient.attemptOperationsWithPostponedFailureRetry(
                    List.of(
                        () -> {
                          attempts.add("x");
                          return new OpaClient.Answer(400, "");
                        },
                        () -> {
                          attempts.add("y");
                          return new OpaClient.Answer(400, "");
                        })));
    assertEquals(recorded.get("all_fail").asText(), raised.getMessage());
    assertEquals(SourceAnswers.strings(recorded.get("all_fail_attempts")), attempts);
  }

  /**
   * R84: a policy write accepts 400 as final — bad rego does not become right on a second try —
   * while any status outside the accepted list is retried and then raised.
   */
  @Test
  void theAcceptedStatusListsAreTheSources() throws Exception {
    try (RecordingEngine engine = new RecordingEngine(Map.of())) {
      OpaClient client = clientFor(engine.url());

      engine.answerWith("PUT /v1/policies/bad.rego", 400);
      client.setPolicy("bad.rego", "not rego", null);
      assertEquals(List.of(List.of("set_policy", "bad.rego")), engine.calls());

      engine.answerWith("DELETE /v1/policies/gone.rego", 404);
      client.deletePolicy("gone.rego", null);

      engine.answerWith("PUT /v1/data/x", 304);
      client.setPolicyData(Rpc.MAPPER.readTree("{\"v\":1}"), "/x", null);

      engine.answerWith("DELETE /v1/data/y", 404);
      client.deletePolicyData("/y", null);

      engine.answerWith("PUT /v1/policies/boom.rego", 500);
      assertThrows(
          IllegalStateException.class, () -> client.setPolicy("boom.rego", "package b\n", null));
    }
  }

  /** R57: clearing the root writes an empty object, because OPA has no delete for the root. */
  @Test
  void deletingTheRootWritesAnEmptyObject() throws Exception {
    try (RecordingEngine engine = new RecordingEngine(Map.of())) {
      OpaClient client = clientFor(engine.url());
      client.deletePolicyData("", null);
      assertEquals(List.of(List.of("set_policy_data", "")), engine.calls());
    }
  }

  /** R85: a mode missing a required setting fails construction, with the source's own message. */
  @Test
  void authenticationModeValidationMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("policy_store_auth");

    assertEquals("ok", recorded.get("none").asText());
    assertNotNull(clientFor("http://127.0.0.1:1", PolicyStoreAuth.NONE, null));

    assertEquals("ok", recorded.get("token_ok").asText());
    assertNotNull(clientFor("http://127.0.0.1:1", PolicyStoreAuth.TOKEN, "T"));

    assertEquals(
        "Exception: required variables for token auth are not set",
        recorded.get("token_missing").asText());
    IllegalStateException token =
        assertThrows(
            IllegalStateException.class,
            () -> clientFor("http://127.0.0.1:1", PolicyStoreAuth.TOKEN, null));
    assertTrue(
        recorded.get("token_missing").asText().endsWith(token.getMessage()),
        "message was: " + token.getMessage());

    assertEquals("ok", recorded.get("oauth_ok").asText());
    assertNotNull(
        new OpaClient(
            "http://127.0.0.1:1", null, PolicyStoreAuth.OAUTH, "id", "secret", "http://oauth",
            true, true, false, null, null, null, List.of(), HEALTH_PATH,
            ConnRetryOptions.defaults()));

    IllegalStateException oauth =
        assertThrows(
            IllegalStateException.class,
            () ->
                new OpaClient(
                    "http://127.0.0.1:1", null, PolicyStoreAuth.OAUTH, "id", null, null, true,
                    true, false, null, null, null, List.of(), HEALTH_PATH,
                    ConnRetryOptions.defaults()));
    assertTrue(recorded.get("oauth_missing").asText().endsWith(oauth.getMessage()));

    IllegalStateException tls =
        assertThrows(
            IllegalStateException.class,
            () ->
                new OpaClient(
                    "http://127.0.0.1:1", null, PolicyStoreAuth.TLS, null, null, null, true, true,
                    false, "cert", null, null, List.of(), HEALTH_PATH,
                    ConnRetryOptions.defaults()));
    assertTrue(recorded.get("tls_missing").asText().endsWith(tls.getMessage()));
  }

  /** R84: a patch is sent with its null fields removed, because OPA refuses an action carrying one. */
  @Test
  void aPatchIsSentWithoutItsNullFields() {
    JsonNode stripped =
        OpaClient.excludeNoneFields(List.of(new Store.JSONPatchAction("add", "/x", 1, null)));
    assertEquals("[{\"op\":\"add\",\"path\":\"/x\",\"value\":1}]", stripped.toString());
  }
}
