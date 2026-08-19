package io.akka.opal.domain;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * One channel's log: the position the next change will get, and the most recent changes kept so a
 * member that was away can be handed exactly what it missed. SPEC-001 §2.4.
 *
 * <p>The original keeps none of this. A change published while a member was away is never
 * delivered and the member is never told — question-log row 9.
 */
public record ChannelState(
    String name, long nextPosition, List<Change> retained, Set<String> members) {

  public ChannelState {
    retained = List.copyOf(retained);
    members = Set.copyOf(members);
  }

  public static ChannelState empty() {
    return new ChannelState("", 1, List.of(), Set.of());
  }

  /**
   * Keep the newest changes and drop the oldest, bounded by count and by size.
   *
   * <p>The size bound is the one that matters: a count on its own says nothing about how
   * much is being held, and a channel's whole state has to stay small enough to be copied
   * between regions. What falls off the end is not lost information — a member that is
   * further back than the oldest change kept is told so rather than handed a short list.
   */
  public ChannelState accept(Change change, int retention, int retainedBytes) {
    var kept = new ArrayList<>(retained);
    kept.add(change);
    while (kept.size() > 1 && (kept.size() > retention || weigh(kept) > retainedBytes)) {
      kept.remove(0);
    }
    return new ChannelState(change.channel(), change.position() + 1, kept, members);
  }

  /** Roughly how much room the kept changes take, counting the parts that carry text. */
  public static int weigh(List<Change> changes) {
    var total = 0;
    for (var change : changes) {
      total += weigh(change);
    }
    return total;
  }

  public static int weigh(Change change) {
    var total = length(change.id()) + length(change.channel()) + length(change.reason());
    for (var entry : change.entries()) {
      total += length(entry.address()) + length(entry.destination()) + length(entry.value());
    }
    return total;
  }

  private static int length(String s) {
    return s == null ? 0 : s.length();
  }

  public ChannelState enrol(String name, String memberId) {
    var joined = new LinkedHashSet<>(members);
    joined.add(memberId);
    return new ChannelState(
        this.name.isEmpty() ? name : this.name, nextPosition, retained, joined);
  }

  /** The position of the oldest change still kept, or 0 when nothing has been published. */
  public long earliestRetained() {
    return retained.isEmpty() ? 0 : retained.get(0).position();
  }

  public List<Change> after(long position) {
    return retained.stream().filter(c -> c.position() > position).toList();
  }
}
