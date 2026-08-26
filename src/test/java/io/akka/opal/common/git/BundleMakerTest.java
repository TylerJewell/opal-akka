package io.akka.opal.common.git;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.schemas.Policy;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R23 to R33, compared against the bundles the original produced from the same two
 * commits. The two commit hashes are redacted to {@code H1} and {@code H2} on both sides, which
 * is what lets the comparison be of the bundle rather than of the repository it came from.
 */
class BundleMakerTest {

  private static ProbeRepository repo;

  @BeforeAll
  static void setUp() throws Exception {
    repo = new ProbeRepository();
  }

  @AfterAll
  static void tearDown() throws Exception {
    repo.close();
  }

  private BundleMaker maker(List<String> directories, List<String> extensions,
      List<String> ignore, String manifestPath) {
    return new BundleMaker(
        repo.repository(), Set.copyOf(directories), extensions, manifestPath, ignore,
        List.of(".rego"));
  }

  private JsonNode redacted(Policy.PolicyBundle bundle) {
    ObjectNode node = (ObjectNode) SourceAnswers.MAPPER.valueToTree(bundle);
    for (String field : List.of("hash", "old_hash")) {
      JsonNode value = node.get(field);
      if (value != null && !value.isNull()) {
        String text = value.asText();
        if (text.equals(repo.first.getName())) {
          node.put(field, "H1");
        } else if (text.equals(repo.second.getName())) {
          node.put(field, "H2");
        }
      }
    }
    return node;
  }

  private void same(String recordedName, Policy.PolicyBundle bundle) {
    JsonNode expected = SourceAnswers.get("bundles").get(recordedName);
    JsonNode actual = redacted(bundle);
    assertEquals(sorted(expected), sorted(actual), recordedName);
  }

  /** Field order differs between the two writers; the content is what is being compared. */
  private static String sorted(JsonNode node) {
    try {
      Object value = SourceAnswers.MAPPER.treeToValue(node, Object.class);
      return SourceAnswers.MAPPER.writerWithDefaultPrettyPrinter()
          .withFeatures(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void completeBundleAtTheFirstCommitMatchesTheSource() throws Exception {
    same("full_first", maker(List.of("."), List.of(".rego", ".json"), List.of(), "")
        .makeBundle(repo.first));
  }

  @Test
  void completeBundleAtTheSecondCommitMatchesTheSource() throws Exception {
    same("full_second", maker(List.of("."), List.of(".rego", ".json"), List.of(), "")
        .makeBundle(repo.second));
  }

  @Test
  void differentialBundleMatchesTheSource() throws Exception {
    same("diff_first_to_second", maker(List.of("."), List.of(".rego", ".json"), List.of(), "")
        .makeDiffBundle(repo.first, repo.second));
  }

  @Test
  void scopingToOneDirectoryMatchesTheSource() throws Exception {
    same("scoped_to_envs", maker(List.of("envs"), List.of(".rego", ".json"), List.of(), "")
        .makeBundle(repo.second));
  }

  @Test
  void bundleIgnoreMatchesTheSource() throws Exception {
    same("bundle_ignore_envs",
        maker(List.of("."), List.of(".rego", ".json"), List.of("envs/**"), "")
            .makeBundle(repo.second));
  }

  @Test
  void filteringToRegoOnlyMatchesTheSource() throws Exception {
    same("rego_extension_only", maker(List.of("."), List.of(".rego"), List.of(), "")
        .makeBundle(repo.second));
  }

  @Test
  void nothingDeletedLeavesDeletedFilesNull() throws Exception {
    Policy.PolicyBundle bundle =
        maker(List.of("."), List.of(".rego", ".json"), List.of(), "").makeBundle(repo.second);
    assertNull(bundle.deleted_files(), "R33");
  }
}
