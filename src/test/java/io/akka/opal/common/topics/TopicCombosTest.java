package io.akka.opal.common.topics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R13 to R19, against the eighteen expansions the source produced.
 *
 * <p>R14 and R15 — the right-most colon, and an empty prefix that is not a prefix — need
 * shapes the source never builds for itself: every topic OPAL makes has its slashes after
 * the last colon, and re-attaching the prefix then reconstructs the same string whichever
 * colon was split on. The five that tell the two readings apart were run against the
 * original and are in the recorded set.
 *
 * <p>The twelve are the class enumerated rather than a sample: the empty topic, a bare slash, a
 * doubled slash, an empty prefix, a trailing colon and two colons all decide different branches
 * of one function, and reading three of them and generalising is how the wrong rule gets
 * written down.
 */
class TopicCombosTest {

  @Test
  void everyRecordedExpansionMatches() {
    JsonNode recorded = SourceAnswers.get("topic_combos");
    for (Iterator<Map.Entry<String, JsonNode>> it = recorded.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> entry = it.next();
      List<String> expected = SourceAnswers.strings(entry.getValue());
      assertEquals(expected, Topics.topicCombos(entry.getKey()), "topic " + entry.getKey());
    }
  }

  @Test
  void policyTopicsMatch() {
    assertEquals(
        SourceAnswers.strings(SourceAnswers.get("policy_topics")),
        Topics.policyTopics(List.of(".", "a", "a/b")));
  }

  @Test
  void removePrefixMatches() {
    JsonNode recorded = SourceAnswers.get("remove_prefix");
    for (Iterator<Map.Entry<String, JsonNode>> it = recorded.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> entry = it.next();
      assertEquals(
          entry.getValue().asText(), Topics.removePrefix(entry.getKey()), entry.getKey());
    }
  }

  @Test
  void directoriesBecomeNonIntersectingPolicyTopics() {
    for (JsonNode row : SourceAnswers.get("pubsub_topics_from_directories")) {
      List<String> input = SourceAnswers.strings(row.get("input"));
      List<String> expected = SourceAnswers.strings(row.get("output"));
      List<String> actual = new ArrayList<>(Topics.pubsubTopicsFromDirectories(input));
      java.util.Collections.sort(actual);
      List<String> sortedExpected = new ArrayList<>(expected);
      java.util.Collections.sort(sortedExpected);
      assertEquals(sortedExpected, actual, "directories " + input);
    }
  }

}
