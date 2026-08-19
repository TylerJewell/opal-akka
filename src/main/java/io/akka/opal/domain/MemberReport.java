package io.akka.opal.domain;

import java.util.Map;
import java.util.Set;

/**
 * Where one member has got to, and what it holds. SPEC-001 rule 19.
 *
 * <p>The original reports a member's identifier, its connection identifiers and the names it
 * watches, and nothing about its position — question-log row 13 — so there is no way to ask
 * whether a fleet is in step.
 */
public record MemberReport(
    String id,
    String channel,
    Set<String> watching,
    long position,
    long applied,
    long duplicates,
    long gaps,
    boolean away,
    Map<String, String> store) {

  public static MemberReport of(MemberState state) {
    return new MemberReport(
        state.id(),
        state.channel(),
        state.watching(),
        state.position(),
        state.applied(),
        state.duplicates(),
        state.gaps(),
        state.away(),
        state.store());
  }
}
