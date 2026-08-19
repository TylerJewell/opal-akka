package io.akka.opal.domain;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * One member of the fleet: what it watches, how far through its channel it has got, and what it
 * holds as a result. SPEC-001 §2.3.
 *
 * <p>{@code position} is the position of the last change this member accounted for. It starts at
 * 0, meaning nothing yet, and only ever goes up.
 */
public record MemberState(
    String id,
    String channel,
    Set<String> watching,
    long position,
    Map<String, String> store,
    long applied,
    long duplicates,
    long gaps,
    boolean joined,
    boolean away) {

  public MemberState {
    watching = Set.copyOf(watching);
    store = Map.copyOf(store);
  }

  public static MemberState empty() {
    return new MemberState("", "", Set.of(), 0, Map.of(), 0, 0, 0, false, false);
  }

  public MemberState join(String id, String channel, Set<String> watching) {
    return new MemberState(
        id, channel, watching, position, store, applied, duplicates, gaps, true, false);
  }

  /**
   * Put every entry of {@code change} into the store, in the order the change lists them, and move
   * to its position. A value at a destination replaces whatever was at that destination or under
   * it — SPEC-001 rules 11-14, open decision D3.
   */
  public MemberState apply(Change change) {
    var next = new LinkedHashMap<>(store);
    for (var entry : change.entries()) {
      if (!entry.addressed().reaches(watching)) {
        continue;
      }
      var at = entry.at();
      next.keySet().removeIf(held -> at.contains(Destination.of(held)));
      next.put(at.path(), entry.value());
    }
    return new MemberState(
        id, channel, watching, change.position(), next, applied + 1, duplicates, gaps, joined, away);
  }

  public MemberState skipTo(long newPosition) {
    return new MemberState(
        id, channel, watching, newPosition, store, applied, duplicates, gaps, joined, away);
  }

  public MemberState countDuplicate() {
    return new MemberState(
        id, channel, watching, position, store, applied, duplicates + 1, gaps, joined, away);
  }

  public MemberState countGap() {
    return new MemberState(
        id, channel, watching, position, store, applied, duplicates, gaps + 1, joined, away);
  }

  public MemberState withAway(boolean nowAway) {
    return new MemberState(
        id, channel, watching, position, store, applied, duplicates, gaps, joined, nowAway);
  }
}
