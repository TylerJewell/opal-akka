package io.akka.opal.common.topics;

import io.akka.opal.common.util.Paths2;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Topic naming and expansion — SPEC-002 R13 to R22. */
public final class Topics {

  public static final String POLICY_PREFIX = "policy:";
  public static final String ALL_TOPICS = "__EventNotifier_ALL_TOPICS__";
  public static final String TOPIC_DELIMITER = "/";
  public static final String PREFIX_DELIMITER = ":";

  private Topics() {}

  /**
   * R13 to R16: the prefixes of a topic, shortest first. A colon splits at the right-most one
   * and the part before it is re-attached to every expansion; an empty prefix is not a prefix,
   * so {@code :y} expands to {@code [y]}.
   */
  public static List<String> topicCombos(String topic) {
    List<String> combos = new ArrayList<>();
    String prefix = null;
    String rest = topic;
    int colon = topic.lastIndexOf(':');
    if (colon >= 0) {
      prefix = topic.substring(0, colon);
      rest = topic.substring(colon + 1);
    }
    String[] subTopics = rest.split(TOPIC_DELIMITER, -1);
    String current = subTopics[0];
    combos.add(withPrefix(prefix, current));
    for (int i = 1; i < subTopics.length; i++) {
      current = current + TOPIC_DELIMITER + subTopics[i];
      combos.add(withPrefix(prefix, current));
    }
    return combos;
  }

  private static String withPrefix(String prefix, String topic) {
    return prefix == null || prefix.isEmpty() ? topic : prefix + PREFIX_DELIMITER + topic;
  }

  /** R17: a directory becomes a policy topic. */
  public static List<String> policyTopics(List<String> paths) {
    List<String> out = new ArrayList<>();
    for (String path : paths) {
      out.add(POLICY_PREFIX + path);
    }
    return out;
  }

  /** R19: strips a leading {@code policy:}, and leaves anything else alone. */
  public static String removePrefix(String topic) {
    return removePrefix(topic, POLICY_PREFIX);
  }

  public static String removePrefix(String topic, String prefix) {
    return topic.startsWith(prefix) ? topic.substring(prefix.length()) : topic;
  }

  /** R18: descendants are dropped before the remaining directories become topics. */
  public static List<String> pubsubTopicsFromDirectories(List<String> directories) {
    Set<String> nonIntersecting = Paths2.nonIntersectingDirectories(directories);
    return policyTopics(new ArrayList<>(nonIntersecting));
  }

  /** R22: the union of every entry's expansion, in first-seen order. */
  public static Set<String> expandAll(List<String> topics) {
    Set<String> out = new LinkedHashSet<>();
    for (String topic : topics) {
      out.addAll(topicCombos(topic));
    }
    return out;
  }
}
