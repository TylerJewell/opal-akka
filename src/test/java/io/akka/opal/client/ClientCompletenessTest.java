package io.akka.opal.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.WireAnswers;
import io.akka.opal.client.pubsub.PubSubClient;
import io.akka.opal.common.fetcher.Retries;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.util.Http;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The client-side behaviours the completeness survey found missing — SPEC-002 R179–R184 and
 * R246–R263.
 */
class ClientCompletenessTest {

  /** R254: an update with no id of its own is given one, and one that has an id keeps it. */
  @Test
  void anUpdateWithNoIdIsGivenOne() {
    Data.DataUpdate without = new Data.DataUpdate(null, List.of(), "reason", null);
    Data.DataUpdate given = ClientRuntime.withId(without);
    assertNotEquals(null, given.id());
    assertEquals(32, given.id().length(), given.id());

    Data.DataUpdate with = new Data.DataUpdate("mine", List.of(), "reason", null);
    assertSame(with, ClientRuntime.withId(with));
  }

  /** R252: a tenant-prefixed topic is matched by the name the client subscribed to. */
  @Test
  void theTenantPrefixIsTakenOffATopic() {
    assertEquals("policy_data", PubSubClient.stripTenantPrefix("app_1::policy_data"));
    assertEquals("policy_data", PubSubClient.stripTenantPrefix("policy_data"));
    assertEquals("a::b", PubSubClient.stripTenantPrefix("app::a::b"));
    assertEquals(null, PubSubClient.stripTenantPrefix(null));
  }

  /** R179 and R181: two hundred attempts, and a wait that grows but may still be nothing. */
  @Test
  void theRetryPolicyIsTheSourcesDefault() {
    Retries.Config defaults = Retries.Config.defaults();
    assertEquals(200, defaults.attempts());
    assertEquals(1.0, defaults.multiplier());

    // The wait is uniform between zero and a ceiling that doubles, so what can be asserted is the
    // ceiling — a fleet arriving together is what the randomness exists to prevent.
    for (int attempt = 1; attempt <= 10; attempt++) {
      double wait = Retries.waitSeconds(defaults, attempt);
      assertTrue(wait >= 0, "attempt " + attempt);
      assertTrue(wait <= Math.pow(2, attempt), "attempt " + attempt + " waited " + wait);
    }
  }

  /** R180: an event carrying its own settings replaces the default rather than adjusting it. */
  @Test
  void anEventCanReplaceTheRetryPolicy() {
    Retries.Config given = Retries.Config.from(Map.of("attempts", 3, "max", 0.5));
    assertEquals(3, given.attempts());
    assertEquals(0.5, given.maxWaitSeconds());
    assertTrue(Retries.waitSeconds(given, 9) <= 0.5, "the ceiling was not applied");
  }

  /** R179: the attempts are made, and the last failure is what the caller is told. */
  @Test
  void everyAttemptIsMadeBeforeTheFailureIsRaised() {
    List<Integer> attempts = new ArrayList<>();
    IllegalStateException raised =
        assertThrows(
            IllegalStateException.class,
            () ->
                Retries.call(
                    new Retries.Config(4, 1, 1),
                    () -> {
                      attempts.add(attempts.size() + 1);
                      throw new IllegalStateException("attempt " + attempts.size());
                    },
                    seconds -> true));
    assertEquals(List.of(1, 2, 3, 4), attempts);
    assertEquals("attempt 4", raised.getMessage());
  }

  /** R179: a call that succeeds is not retried. */
  @Test
  void aSuccessIsNotRetried() {
    List<Integer> attempts = new ArrayList<>();
    String answer =
        Retries.call(
            Retries.Config.defaults(),
            () -> {
              attempts.add(1);
              return "ok";
            },
            seconds -> true);
    assertEquals("ok", answer);
    assertEquals(1, attempts.size());
  }

  /** R184: both entries have to say yes before anything extra is trusted. */
  @Test
  void extraTrustNeedsBothEntriesAndAFileThatExists() {
    assertSame(Http.plain(), Http.clientFor(false, "/etc/ssl/ca.pem"));
    assertSame(Http.plain(), Http.clientFor(true, null));
    assertSame(Http.plain(), Http.clientFor(true, ""));
    assertSame(
        Http.plain(),
        Http.clientFor(true, "/no/such/file/anywhere.pem"),
        "a path that does not resolve leaves the default trust in place");
  }

  /** R258: what a patch at the root becomes, against what the source sent. */
  @Test
  void aPatchAtTheRootIsWrappedTheWayTheSourceWrapsIt() {
    JsonNode recorded = WireAnswers.get("opa_requests");
    assertEquals(
        "{\"items\": [{\"op\": \"add\", \"path\": \"/x\", \"value\": 1}]}",
        recorded.get("patch_root").get(0).get("body").asText(),
        "the recorded answer no longer shows the wrapping");
    assertEquals(
        "[{\"op\": \"add\", \"path\": \"/x\", \"value\": 1}]",
        recorded.get("patch_under_path").get(0).get("body").asText());
    assertEquals(
        "{\"items\": [1, 2, 3]}", recorded.get("put_root_list").get(0).get("body").asText());
  }

  /** R262: the client's own connection-retry settings become the engine's default policy. */
  @Test
  void theClientsRetryEntryReachesTheFetchingEngine() {
    io.akka.opal.common.config.Options.ConnRetryOptions options =
        new io.akka.opal.common.config.Options.ConnRetryOptions(null, 3.0, 5, 30.0);
    Retries.Config policy = ClientRuntime.retryPolicy(options);
    assertEquals(5, policy.attempts());
    assertEquals(3.0, policy.multiplier());
    assertEquals(30.0, policy.maxWaitSeconds());
    assertEquals(Retries.Config.defaults(), ClientRuntime.retryPolicy(null));
  }
}
