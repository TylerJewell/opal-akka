package io.akka.opal.client.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-002 R79 to R81, and the static-data cache behind offline mode's backup. */
class PolicyStoreTest {

  /**
   * R79, over the six modules the source's probe filtered. Three survive: an ordinary one, one
   * with no parsable package at all, and one whose package merely starts with the letters of
   * {@code system} — the exclusion is on the prefix {@code system.} and not on the word.
   */
  @Test
  void moduleFilteringMatchesTheSource() {
    ObjectNode result = Rpc.MAPPER.createObjectNode();
    ArrayNode policies = result.putArray("result");
    policies.addObject().put("id", "a.rego").put("raw", "package a\n");
    policies.addObject().put("id", "b.rego").put("raw", "package system.authz\n");
    policies.addObject().put("id", "c.rego").put("raw", "package system.opal\n");
    policies.addObject().put("id", "d.rego").put("raw", "no package here\n");
    policies.addObject().put("id", "e.rego").put("raw", "package systemic.thing\n");
    policies.addObject().put("id", "engine/healthcheck/opal.rego").put("raw", "package x\n");

    List<String> kept =
        new ArrayList<>(
            OpaClient.extractModulesFromPoliciesJson(result, "engine/healthcheck/opal.rego")
                .keySet());
    assertEquals(SourceAnswers.strings(SourceAnswers.get("extract_modules_from_policies_json")),
        kept);
  }

  /** R81, over the seven paths the source's own normaliser answered. */
  @Test
  void dataModulePathNormalisationMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("safe_data_module_path");
    assertEquals(recorded.get("''").asText(), OpaClient.safeDataModulePath(""));
    assertEquals(recorded.get("'.'").asText(), OpaClient.safeDataModulePath("."));
    assertEquals(recorded.get("'/a'").asText(), OpaClient.safeDataModulePath("/a"));
    assertEquals(recorded.get("'/a/b'").asText(), OpaClient.safeDataModulePath("/a/b"));
    assertEquals(recorded.get("'a'").asText(), OpaClient.safeDataModulePath("a"));
    assertEquals(recorded.get("'a/b'").asText(), OpaClient.safeDataModulePath("a/b"));
    assertEquals(recorded.get("None").asText(), OpaClient.safeDataModulePath(null));
  }

  /** R80: manifest order decides, an unnamed module sorts last, and ties keep their order. */
  @Test
  void bundleLoadOrderMatchesTheSource() {
    Policy.PolicyBundle bundle =
        new Policy.PolicyBundle(
            List.of("b.rego", "a.rego", "d"),
            "h",
            "old",
            List.of(new Policy.DataModule("d", "{}"), new Policy.DataModule("z", "{}")),
            List.of(
                new Policy.RegoModule("a.rego", "a", ""),
                new Policy.RegoModule("b.rego", "b", ""),
                new Policy.RegoModule("c.rego", "c", "")),
            new Policy.DeletedFiles(List.of("x"), List.of("p.rego", "q.rego")));

    JsonNode recorded = SourceAnswers.get("bundle_utils_sorting");
    List<String> policyToLoad = new ArrayList<>();
    BundleUtils.sortedPolicyModulesToLoad(bundle).forEach(m -> policyToLoad.add(m.path()));
    List<String> dataToLoad = new ArrayList<>();
    BundleUtils.sortedDataModulesToLoad(bundle).forEach(m -> dataToLoad.add(m.path()));

    assertEquals(SourceAnswers.strings(recorded.get("policy_to_load")), policyToLoad);
    assertEquals(SourceAnswers.strings(recorded.get("data_to_load")), dataToLoad);
    assertEquals(
        SourceAnswers.strings(recorded.get("policy_to_delete")),
        BundleUtils.sortedPolicyModulesToDelete(bundle));
    assertEquals(
        SourceAnswers.strings(recorded.get("data_to_delete")),
        BundleUtils.sortedDataModulesToDelete(bundle));
  }

  /** The static-data cache's five-step trail, which offline mode's backup is taken from. */
  @Test
  void staticDataCacheMatchesTheSource() throws Exception {
    JsonNode recorded = SourceAnswers.get("opa_static_cache");
    OpaStaticDataCache cache = new OpaStaticDataCache();

    cache.set("", Rpc.MAPPER.readTree("{\"a\": {\"b\": 1}}"));
    assertEquals(recorded.get(0), cache.getData(), "after set root");

    cache.set("/a/c", Rpc.MAPPER.readTree("2"));
    assertEquals(recorded.get(1), cache.getData(), "after set /a/c");

    cache.patch(
        "/a",
        List.of(new Store.JSONPatchAction("add", "/d", 3, null)));
    assertEquals(recorded.get(2), cache.getData(), "after patch /a");

    cache.delete("/a/b");
    assertEquals(recorded.get(3), cache.getData(), "after delete /a/b");

    cache.delete("/");
    assertEquals(recorded.get(4), cache.getData(), "after delete root");
  }

  /**
   * R78: an operation that fails is retried at the end, and a round in which every one failed is
   * where it gives up. Two operations, the first of which only succeeds once the second has run,
   * is the shape that makes a renamed policy work.
   */
  @Test
  void postponedRetryLetsAnOutOfOrderPairSucceed() {
    boolean[] secondRan = {false};
    List<Integer> attempts = new ArrayList<>();

    OpaClient.Operation first =
        () -> {
          attempts.add(1);
          return new OpaClient.Answer(secondRan[0] ? 200 : 400, "");
        };
    OpaClient.Operation second =
        () -> {
          attempts.add(2);
          secondRan[0] = true;
          return new OpaClient.Answer(200, "");
        };
    OpaClient.attemptOperationsWithPostponedFailureRetry(List.of(first, second));
    assertEquals(List.of(1, 2, 1), attempts);
  }

  @Test
  void everyOperationFailingIsAnError() {
    assertThrows(
        IllegalStateException.class,
        () ->
            OpaClient.attemptOperationsWithPostponedFailureRetry(
                List.of(
                    () -> new OpaClient.Answer(400, ""), () -> new OpaClient.Answer(400, ""))));
  }
}
