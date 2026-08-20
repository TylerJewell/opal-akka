package io.akka.opal.domain;

import java.util.List;

/**
 * One change to one destination, with its place in that destination's sequence.
 *
 * <p>SPEC-001 §2. The identity and the sequence are assigned by the destination, not by the
 * publisher and not by a subscriber — that is what lets the fleet agree on which change is
 * which (§4 OD-1, OD-3).
 *
 * @param id {@code <destination>#<sequence>}
 * @param topics already expanded to every ancestor, so a subscriber matches by string equality
 * @param payload the value to write, as JSON text. Any JSON value, including a bare number or
 *     string (rule 9), up to {@link DestinationState#MAX_PAYLOAD_BYTES}.
 * @param reason free text from the publisher, never interpreted and never null
 * @param batch the publish that carried this change, when several were published together
 */
public record Change(
    String id,
    String destination,
    long sequence,
    List<String> topics,
    String payload,
    String reason,
    String batch) {

  public static String identify(String destination, long sequence) {
    return destination + "#" + sequence;
  }
}
