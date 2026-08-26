package io.akka.opal.client.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-002 R82 and R83, against the eight cases the source's own matcher answered. */
class IgnorePathsTest {

  @Test
  void ignoringMatchesTheSource() {
    for (JsonNode row : SourceAnswers.get("should_ignore_path")) {
      String path = row.get("path").asText();
      List<String> ignore = SourceAnswers.strings(row.get("ignore"));
      assertEquals(
          row.get("output").asBoolean(),
          IgnorePaths.shouldIgnorePath(path, ignore),
          path + " against " + ignore);
    }
  }

  /** The negation wins whichever order the two patterns were written in. */
  @Test
  void aNegationWinsRegardlessOfOrder() {
    assertEquals(false, IgnorePaths.shouldIgnorePath("a/b.rego", List.of("!a/b.rego", "a/**")));
    assertEquals(false, IgnorePaths.shouldIgnorePath("a/b.rego", List.of("a/**", "!a/b.rego")));
    assertEquals(true, IgnorePaths.shouldIgnorePath("a/c.rego", List.of("a/**", "!a/b.rego")));
  }
}
