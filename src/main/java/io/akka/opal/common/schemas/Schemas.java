package io.akka.opal.common.schemas;

/** What a schema refuses, and the shape a caller sees when it does — SPEC-002 R142 to R144. */
public final class Schemas {

  private Schemas() {}

  /**
   * A value that does not satisfy the schema it was given to.
   *
   * <p>Raised from a record's own constructor rather than from a separate validate step, so a
   * value that exists is a value that passed: the source's schema library works the same way, and
   * an endpoint that accepts one of these bodies answers 422 rather than letting a half-checked
   * object through.
   */
  public static final class ValidationFailure extends RuntimeException {
    public ValidationFailure(String message) {
      super(message);
    }
  }
}
