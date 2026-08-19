package io.akka.opal.domain;

/** What a member did with a change it was handed. SPEC-001 rules 8-10. */
public record Delivery(Outcome outcome, long position, String detail) {

  public enum Outcome {
    /** The change was next in line and its entries were put into the store. */
    APPLIED,
    /** The change was at or behind where the member already is, so nothing changed. */
    DUPLICATE,
    /** The change was more than one past the member, so it was not applied and the position stayed. */
    GAP,
    /** The change was next in line but addressed nowhere this member watches. */
    NOT_WATCHED,
    /** This member is off the fleet, so nothing was handed to it. */
    AWAY
  }

  public static Delivery applied(long position) {
    return new Delivery(Outcome.APPLIED, position, "");
  }

  public static Delivery duplicate(long position) {
    return new Delivery(Outcome.DUPLICATE, position, "");
  }

  public static Delivery notWatched(long position) {
    return new Delivery(Outcome.NOT_WATCHED, position, "");
  }

  public static Delivery away(long position) {
    return new Delivery(Outcome.AWAY, position, "");
  }

  public static Delivery gap(long position, long received) {
    return new Delivery(
        Outcome.GAP, position, "expected " + (position + 1) + ", was handed " + received);
  }
}
