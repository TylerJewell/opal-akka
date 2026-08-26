package io.akka.opal.common.git;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.schemas.Policy;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R28 to R30 and R38 to R39.
 *
 * <p>The five manifest paths are the class enumerated: the empty string, an explicit file, the
 * repository root, a directory holding its own manifest, and one that names nothing. Three of them
 * produce one order and two produce another, and reading two of the five would have got it wrong.
 */
class ManifestTest {

  private static ProbeRepository repo;

  @BeforeAll
  static void setUp() throws Exception {
    repo = new ProbeRepository();
  }

  @AfterAll
  static void tearDown() throws Exception {
    repo.close();
  }

  private BundleMaker maker(String manifestPath) {
    return new BundleMaker(
        repo.repository(),
        Set.of("."),
        List.of(".rego", ".json"),
        manifestPath,
        List.of(),
        List.of(".rego"));
  }

  @Test
  void everyRootManifestPathMatchesTheSource() throws Exception {
    JsonNode recorded = SourceAnswers.get("explicit_manifest");
    for (String path : List.of("", ".manifest", ".", "envs", "nonexistent")) {
      Policy.PolicyBundle bundle = maker(path).makeBundle(repo.second);
      String key = path.isEmpty() ? "<empty>" : path;
      assertEquals(SourceAnswers.strings(recorded.get(key)), bundle.manifest(), "manifest " + key);
    }
  }

  /** R38: two equal commits announce every directory holding a tracked file. */
  @Test
  void policyUpdateNotificationsMatchTheSource() throws Exception {
    JsonNode recorded = SourceAnswers.get("policy_update_notifications");

    Policy.PolicyUpdateMessageNotification first =
        PolicyUpdates.createPolicyUpdate(
            repo.repository(), repo.first, repo.second, List.of(".rego", ".json"), List.of());
    assertEquals(
        SourceAnswers.strings(recorded.get("first_to_second").get("topics")), first.topics());
    assertEquals(
        SourceAnswers.strings(
            recorded.get("first_to_second").get("update").get("changed_directories")),
        first.update().changed_directories());

    Policy.PolicyUpdateMessageNotification same =
        PolicyUpdates.createPolicyUpdate(
            repo.repository(), repo.second, repo.second, List.of(".rego", ".json"), List.of());
    assertEquals(SourceAnswers.strings(recorded.get("same_commit").get("topics")), same.topics());
    assertEquals(
        SourceAnswers.strings(recorded.get("same_commit").get("update").get("changed_directories")),
        same.update().changed_directories());
  }

  /** R39: two different commits with no tracked file between them announce nothing at all. */
  @Test
  void aCommitTouchingNothingTrackedAnnouncesNothing() throws Exception {
    assertNull(
        PolicyUpdates.createPolicyUpdate(
            repo.repository(), repo.first, repo.second, List.of(".nothing"), List.of()),
        "no tracked file changed, so there is nothing to publish");
  }
}
