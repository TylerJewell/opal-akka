package io.akka.opal.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SPEC-002 R18, R21 and R30, against the paths the source's own {@code PathUtils} produced. */
class PathUtilsTest {

  @Test
  void intermediateDirectoriesMatch() {
    for (JsonNode row : SourceAnswers.get("intermediate_directories")) {
      List<String> input = SourceAnswers.strings(row.get("input"));
      assertEquals(
          SourceAnswers.strings(row.get("output")),
          Paths2.intermediateDirectories(input),
          "paths " + input);
    }
  }

  @Test
  void isChildOfDirectoriesMatches() {
    for (JsonNode row : SourceAnswers.get("is_child_of_directories")) {
      Set<String> directories = new LinkedHashSet<>(SourceAnswers.strings(row.get("dirs")));
      assertEquals(
          row.get("output").asBoolean(),
          Paths2.isChildOfDirectories(row.get("path").asText(), directories),
          row.get("path").asText() + " under " + directories);
    }
  }

  @Test
  void nonIntersectingDirectoriesMatch() {
    for (JsonNode row : SourceAnswers.get("non_intersecting_directories")) {
      List<String> input = SourceAnswers.strings(row.get("input"));
      List<String> expected = new ArrayList<>(SourceAnswers.strings(row.get("output")));
      List<String> actual = new ArrayList<>(Paths2.nonIntersectingDirectories(input));
      java.util.Collections.sort(expected);
      java.util.Collections.sort(actual);
      assertEquals(expected, actual, "directories " + input);
    }
  }

  @Test
  void explicitSortingKeepsEveryPath() {
    for (JsonNode row : SourceAnswers.get("sort_paths_according_to_explicit_sorting")) {
      List<String> unsorted = SourceAnswers.strings(row.get("unsorted"));
      List<String> explicit = SourceAnswers.strings(row.get("explicit"));
      assertEquals(
          SourceAnswers.strings(row.get("output")),
          Paths2.sortAccordingToExplicitSorting(unsorted, explicit),
          unsorted + " by " + explicit);
    }
  }

  @Test
  void globStyleMatchingMatches() {
    for (JsonNode row : SourceAnswers.get("glob_style_match_path_to_list")) {
      List<String> patterns = SourceAnswers.strings(row.get("match_paths"));
      JsonNode expected = row.get("output");
      String actual = Glob.globStyleMatchPathToList(row.get("path").asText(), patterns);
      assertEquals(
          expected.isNull() ? null : expected.asText(),
          actual,
          row.get("path").asText() + " against " + patterns);
    }
  }
}
