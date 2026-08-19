package io.akka.opal.domain;

import java.time.Instant;
import java.util.List;

/**
 * A change, once it has been given a position. SPEC-001 §2.2.
 *
 * <p>{@code position} is what the original's change has no equivalent of: its own change carries
 * an identifier, entries, a reason and a callback list, and nothing that says where in the order
 * it belongs — question-log row 5. Everything a member can promise rests on this field.
 */
public record Change(
    String id,
    String channel,
    long position,
    String reason,
    List<Entry> entries,
    Instant publishedAt) {

  public Change {
    entries = List.copyOf(entries);
  }

  /** Whether any of this change's entries reaches a member watching {@code watched}. */
  public boolean reaches(java.util.Collection<String> watched) {
    return entries.stream().anyMatch(e -> e.addressed().reaches(watched));
  }
}
