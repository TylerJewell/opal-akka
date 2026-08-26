package io.akka.opal.common.rego;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-002 R24 to R26, against the package names and module kinds the source reported. */
class RegoTest {

  /** The ten samples the source's probe parsed, keyed the same way. */
  private static final Map<String, String> SAMPLES =
      Map.ofEntries(
          Map.entry("simple", "package foo\n\nallow = true\n"),
          Map.entry("dotted", "package a.b.c\n"),
          Map.entry("quoted", "package a[\"b\"]\n"),
          Map.entry("leading_blank", "\n\npackage x\n"),
          Map.entry("comment_first", "# a comment\npackage y.z\n"),
          Map.entry("indented", "  package indented\n"),
          Map.entry("trailing_space", "package trailing \n"),
          Map.entry("no_package", "allow = true\n"),
          Map.entry("empty", ""),
          Map.entry("package_then_more", "package first\npackage second\n"));

  @Test
  void packageNamesMatchTheSource() {
    JsonNode recorded = SourceAnswers.get("get_rego_package");
    for (Map.Entry<String, String> sample : SAMPLES.entrySet()) {
      JsonNode expected = recorded.get(sample.getKey());
      assertEquals(
          expected.isNull() ? null : expected.asText(),
          Rego.getRegoPackage(sample.getValue()),
          sample.getKey());
    }
  }

  @Test
  void moduleKindMatchesTheSource() {
    for (JsonNode row : SourceAnswers.get("module_kind")) {
      String path = row.get("path").asText();
      assertEquals(row.get("is_data_module").asBoolean(), Rego.isDataModule(path), path);
      assertEquals(
          row.get("is_policy_module").asBoolean(),
          Rego.isPolicyModule(path, List.of(".rego")),
          path);
    }
  }
}
