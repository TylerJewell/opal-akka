package io.akka.opal.domain;

/** What a subscriber did with a change it was handed — SPEC-001 §3 rules 11 to 13. */
public enum Outcome {
  /** The change was next in sequence and was written. */
  APPLIED,
  /** The change was at or below what this subscriber had already applied. */
  DUPLICATE,
  /** The change was further ahead than the next one, so it was not written. */
  GAP
}
