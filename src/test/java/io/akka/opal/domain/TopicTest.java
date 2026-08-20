package io.akka.opal.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 1 to 3 — how a change is addressed, and who it reaches. */
class TopicTest {

  @Test
  void aTopicExpandsToItselfAndEveryAncestor() {
    assertThat(Topics.expand("policy_data/users/keys"))
        .containsExactly("policy_data", "policy_data/users", "policy_data/users/keys");
  }

  @Test
  void aSingleSegmentExpandsToItself() {
    assertThat(Topics.expand("policy_data")).containsExactly("policy_data");
  }

  @Test
  void onlyTheLastColonMarksTheScope() {
    assertThat(Topics.expand("data:policy_data/users"))
        .containsExactly("data:policy_data", "data:policy_data/users");
    assertThat(Topics.expand("a:b:c/d")).containsExactly("a:b:c", "a:b:c/d");

    // The case that tells the two apart. Splitting at the first colon instead would put
    // "b/c" in the path and expand it; splitting at the last leaves one topic, because
    // everything before that colon is scope. Checked against the source, which answers
    // the same — probes/probe_01_topic_expansion.py.
    assertThat(Topics.expand("a:b/c:d")).containsExactly("a:b/c:d");
    assertThat(Topics.expand("a/b:c/d")).containsExactly("a/b:c", "a/b:c/d");
  }

  @Test
  void expandingSeveralTopicsKeepsEachOnce() {
    assertThat(Topics.expandAll(List.of("a/b", "a/c")))
        .containsExactly("a", "a/b", "a/c");
  }

  @Test
  void aBranchSubscriptionIsAddressedByALeafChange() {
    List<String> addressed = Topics.expandAll(List.of("policy_data/users/keys"));
    assertThat(Topics.addresses(addressed, List.of("policy_data"))).isTrue();
    assertThat(Topics.addresses(addressed, List.of("policy_data/users"))).isTrue();
  }

  @Test
  void matchingIsStringEqualityAndNotAPrefixTest() {
    // Rule 3. The source's own destination lock uses a raw prefix and so treats these two as
    // the same place (question-log row 5); addressing must not repeat that mistake.
    List<String> addressed = Topics.expandAll(List.of("users_meta/a"));
    assertThat(Topics.addresses(addressed, List.of("users"))).isFalse();
    assertThat(Topics.addresses(addressed, List.of("users_meta"))).isTrue();
  }

  @Test
  void aSubscriberWithNoTopicsIsAddressedByNothing() {
    assertThat(Topics.addresses(Topics.expandAll(List.of("a/b")), List.of())).isFalse();
  }
}
