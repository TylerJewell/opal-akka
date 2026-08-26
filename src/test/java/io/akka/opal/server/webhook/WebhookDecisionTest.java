package io.akka.opal.server.webhook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.common.config.Options.GitWebhookRequestParams;
import io.akka.opal.common.config.Options.SecretTypeEnum;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R121, R124 and R125 — what a webhook amounts to, over every combination.
 *
 * <p>Three switches decide it and each has two answers: the branch is enforced or not, the
 * repository matches or not, the event is the push event or not. All eight are here, because the
 * three interact — a webhook for the wrong branch is ignored whatever its event says, and one for
 * the wrong repository is ignored unless {@code match_sender_url} is off, which is the switch a
 * deployment behind a proxy turns off and then finds it is receiving everyone's pushes.
 */
class WebhookDecisionTest {

  private static final String REPO = "https://github.com/o/r.git";

  private static GitWebhookRequestParams params(boolean matchSenderUrl) {
    return new GitWebhookRequestParams(
        "x-hub-signature-256",
        SecretTypeEnum.signature,
        "(?<= sha256=).*",
        "x-github-event",
        null,
        "push",
        matchSenderUrl);
  }

  private static Webhooks.GitChanges changes(String repo, String branch) {
    return new Webhooks.GitChanges(List.of(repo), branch, List.of());
  }

  /** R124: the wrong branch answers with no body at all, whatever else is true of it. */
  @Test
  void branchEnforcementOverridesEverything() {
    for (String event : List.of("push", "ping")) {
      Webhooks.Decision decision =
          Webhooks.decide(changes(REPO, "feature"), event, REPO, params(true), true, "main");
      assertTrue(decision.ignoredBranch(), event);
      assertFalse(decision.trigger(), event);
      assertNull(decision.status(), event);
    }
  }

  /** R124: with enforcement off, the same webhook is read normally. */
  @Test
  void withEnforcementOffTheBranchIsNotRead() {
    Webhooks.Decision decision =
        Webhooks.decide(changes(REPO, "feature"), "push", REPO, params(true), false, "main");
    assertFalse(decision.ignoredBranch());
    assertEquals("ok", decision.status());
    assertTrue(decision.trigger());
  }

  /** R124: the right branch passes the gate even with enforcement on. */
  @Test
  void theEnforcedBranchIsLetThrough() {
    Webhooks.Decision decision =
        Webhooks.decide(changes(REPO, "main"), "push", REPO, params(true), true, "main");
    assertFalse(decision.ignoredBranch());
    assertTrue(decision.trigger());
    assertEquals(REPO, decision.repoUrl());
  }

  /** R121: only the configured push event triggers; anything else still answers ok. */
  @Test
  void onlyThePushEventTriggers() {
    Webhooks.Decision push =
        Webhooks.decide(changes(REPO, "main"), "push", REPO, params(true), false, "main");
    assertTrue(push.trigger());
    assertEquals("push", push.event());

    Webhooks.Decision ping =
        Webhooks.decide(changes(REPO, "main"), "ping", REPO, params(true), false, "main");
    assertFalse(ping.trigger());
    assertEquals("ok", ping.status(), "the route still answers, it just does nothing");
    assertEquals("ping", ping.event());
    assertEquals(REPO, ping.repoUrl());
  }

  /** R125: a webhook naming another repository is ignored, and says which event it was. */
  @Test
  void anotherRepositoryIsIgnored() {
    Webhooks.Decision decision =
        Webhooks.decide(
            changes("https://github.com/someone/else.git", "main"), "push", REPO, params(true),
            false, "main");
    assertEquals("ignored", decision.status());
    assertFalse(decision.trigger());
    assertNull(decision.repoUrl(), "an ignored webhook does not echo the tracked repository");
    assertEquals("push", decision.event());
  }

  /** R125: with {@code match_sender_url} off, any repository triggers the pull. */
  @Test
  void withMatchSenderUrlOffAnyRepositoryTriggers() {
    Webhooks.Decision decision =
        Webhooks.decide(
            changes("https://github.com/someone/else.git", "main"), "push", REPO, params(false),
            false, "main");
    assertEquals("ok", decision.status());
    assertTrue(decision.trigger());
    assertEquals(REPO, decision.repoUrl());
  }

  /** With no repository configured at all, nothing matches — even with the sender check off. */
  @Test
  void withNoTrackedRepositoryNothingMatches() {
    for (boolean matchSenderUrl : List.of(true, false)) {
      Webhooks.Decision decision =
          Webhooks.decide(changes(REPO, "main"), "push", "", params(matchSenderUrl), false, "main");
      assertEquals("ignored", decision.status(), "match_sender_url=" + matchSenderUrl);
    }
  }
}
