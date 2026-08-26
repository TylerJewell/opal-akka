package io.akka.opal.common.auth;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A 401 carrying its reason — SPEC-002 R71. The body is {@code {"error": "<reason>"}} and the
 * response carries {@code WWW-Authenticate: Bearer}.
 */
public class Unauthorized extends RuntimeException {

  private final Map<String, Object> detail = new LinkedHashMap<>();

  public Unauthorized(String description) {
    super(description);
    detail.put("error", description);
  }

  public Unauthorized(String description, String token) {
    this(description);
    if (token != null) {
      detail.put("token", token);
    }
  }

  public Map<String, Object> detail() {
    return detail;
  }

  /** The body a caller sees: the token is never part of it. */
  public Map<String, Object> body() {
    Map<String, Object> body = new LinkedHashMap<>(detail);
    body.remove("token");
    return body;
  }
}
