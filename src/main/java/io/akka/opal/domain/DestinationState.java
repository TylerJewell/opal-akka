package io.akka.opal.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A destination, and the sequence of changes made to it — SPEC-001 §2, §3 rules 5, 6, 16, 18, 20.
 *
 * <p>The retained log is bounded, but nothing is lost by that bound: a subscriber whose gap
 * reaches further back than the log can still take {@link #value()} and {@link #sequence()}
 * in one step and be exactly up to date (rule 18). This is the difference between a bounded
 * log and the bounded replay buffer the source keeps, which drops the oldest change and has
 * nothing else holding it.
 *
 * <p>The bound is on bytes as well as on count. A count alone does not bound the state: the
 * runtime replicates entity state only while it stays under a megabyte, and a thousand
 * changes of any interesting size is past that.
 *
 * @param sequence the number of the last change accepted here; 0 before the first
 * @param value what the last accepted change left here; absent before the first
 */
public record DestinationState(
    String destination, long sequence, List<Change> retained, Optional<String> value) {

  /** The most changes kept for subscribers that fell behind. */
  public static final int RETAINED = 1000;

  /** And the most payload bytes, which is the bound that actually holds the state down. */
  public static final int RETAINED_BYTES = 256 * 1024;

  /** A single change larger than this is refused: it could not be retained even alone. */
  public static final int MAX_PAYLOAD_BYTES = 64 * 1024;

  public static DestinationState empty(String destination) {
    return new DestinationState(destination, 0, List.of(), Optional.empty());
  }

  public long next() {
    return sequence + 1;
  }

  public DestinationState withAccepted(Change change) {
    List<Change> log = new ArrayList<>(retained);
    log.add(change);
    long bytes = log.stream().mapToLong(c -> c.payload().length()).sum();
    while (log.size() > RETAINED || (log.size() > 1 && bytes > RETAINED_BYTES)) {
      bytes -= log.remove(0).payload().length();
    }
    return new DestinationState(
        destination, change.sequence(), List.copyOf(log), Optional.of(change.payload()));
  }

  /**
   * The changes after {@code after}, in sequence order — rule 16.
   *
   * <p>Empty when the caller is already up to date. {@link #canServeFrom} says whether an
   * empty answer means "nothing to send" or "that is further back than the log goes".
   */
  public List<Change> since(long after) {
    return retained.stream().filter(c -> c.sequence() > after).toList();
  }

  /** Whether the log still reaches back far enough to answer {@link #since} completely. */
  public boolean canServeFrom(long after) {
    if (after >= sequence) {
      return true;
    }
    return !retained.isEmpty() && retained.get(0).sequence() <= after + 1;
  }

  /** The oldest change still kept, or 0 when nothing is. */
  public long oldestRetained() {
    return retained.isEmpty() ? 0 : retained.get(0).sequence();
  }
}
