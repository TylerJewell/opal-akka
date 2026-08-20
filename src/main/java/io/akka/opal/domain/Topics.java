package io.akka.opal.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * How a change is addressed to part of a fleet — SPEC-001 §3 rules 1 to 3.
 *
 * <p>A topic names a place in a tree. Addressing a change to {@code a/b/c} also addresses it
 * to everyone watching {@code a} and {@code a/b}, so a subscriber can ask for a whole branch
 * without naming every leaf under it.
 *
 * <p>A colon marks a scope: everything before the last colon is carried onto each expanded
 * topic unchanged. Only the last colon counts, so a scope may itself contain one.
 */
public final class Topics {

  private static final String SEGMENT = "/";
  private static final char SCOPE = ':';

  private Topics() {}

  /** Rules 1 and 2: one topic, expanded to itself and every ancestor, coarsest first. */
  public static List<String> expand(String topic) {
    String scope = "";
    String path = topic;
    int mark = topic.lastIndexOf(SCOPE);
    if (mark >= 0) {
      scope = topic.substring(0, mark + 1);
      path = topic.substring(mark + 1);
    }

    List<String> expanded = new ArrayList<>();
    StringBuilder walked = new StringBuilder();
    for (String segment : path.split(SEGMENT, -1)) {
      if (!walked.isEmpty()) {
        walked.append(SEGMENT);
      }
      walked.append(segment);
      expanded.add(scope + walked);
    }
    return expanded;
  }

  /**
   * Every topic of a change, expanded and de-duplicated.
   *
   * <p>The order is first-seen, then coarsest-first within each topic. The source builds this
   * as an unordered set; keeping an order costs nothing and makes a delivery reproducible.
   */
  public static List<String> expandAll(Collection<String> topics) {
    Set<String> all = new LinkedHashSet<>();
    for (String topic : topics) {
      all.addAll(expand(topic));
    }
    return List.copyOf(all);
  }

  /**
   * Rule 3: a subscriber is addressed when one of its own topics is exactly one of the
   * change's expanded topics. String equality, never a prefix test — the expansion above is
   * what makes a coarse subscription match a fine change, and doing it again here would make
   * {@code users_meta} match a subscription to {@code users}.
   */
  public static boolean addresses(Collection<String> expandedTopics, Collection<String> subscribed) {
    for (String topic : expandedTopics) {
      if (subscribed.contains(topic)) {
        return true;
      }
    }
    return false;
  }
}
