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

  /**
   * R269: a field the source types as an http url, checked the way its schema library checks one.
   *
   * <p>A scheme of {@code http} or {@code https} and a host. The two messages are the library's
   * own, because they reach a caller as the body of a 422.
   */
  public static void requireHttpUrl(String value, String field) {
    java.net.URI parsed;
    try {
      parsed = new java.net.URI(value);
    } catch (java.net.URISyntaxException e) {
      throw new ValidationFailure(field + ": invalid or missing URL scheme");
    }
    String scheme = parsed.getScheme();
    if (scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
      throw new ValidationFailure(field + ": invalid or missing URL scheme");
    }
    if (parsed.getHost() == null || parsed.getHost().isEmpty()) {
      throw new ValidationFailure(field + ": URL host invalid");
    }
  }
}
