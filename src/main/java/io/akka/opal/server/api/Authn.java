package io.akka.opal.server.api;

import akka.javasdk.http.RequestContext;
import io.akka.opal.common.auth.JwtVerifier;
import io.akka.opal.common.auth.Unauthorized;
import java.util.Map;

/**
 * Reading the caller's identity off a request — SPEC-002 R70 to R73.
 *
 * <p>With verification disabled every route is open and this returns no claims, which is what
 * lets OPAL run with no keys at all. Every authorisation check downstream is written to allow in
 * that state, so this is the one place the decision is made.
 */
public final class Authn {

  private Authn() {}

  public static String bearerToken(RequestContext context) {
    return context
        .requestHeader("Authorization")
        .map(header -> JwtVerifier.tokenFromHeader(header.value()))
        .orElse(null);
  }

  /**
   * R375: a refusal is logged with its reason and without the token.
   *
   * <p>Every route answers the same 401 body, so from outside a bad signature, an expired token
   * and a subject that is not a uuid are one answer. The reason is the only thing that tells an
   * operator which of them happened, and the token itself is dropped from the line because a
   * log is not where a credential belongs.
   */
  public static Map<String, Object> requireLoggedIn(JwtVerifier verifier, RequestContext context) {
    try {
      return verifier.verifyLoggedIn(bearerToken(context));
    } catch (Unauthorized e) {
      LOG.error("Authentication failed with 401 due to error: {}", e.body());
      throw e;
    }
  }

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Authn.class);

  /** R72: the master token as a static credential; with none configured the route is open. */
  public static void requireMasterToken(String masterToken, RequestContext context) {
    if (masterToken == null) {
      return;
    }
    String header =
        context.requestHeader("Authorization").map(h -> h.value()).orElse(null);
    if (header == null) {
      throw new Unauthorized("Authorization header is required!");
    }
    String token = JwtVerifier.tokenFromHeader(header);
    if (token == null || !token.equals(masterToken)) {
      Unauthorized refused = new Unauthorized("unauthorized to access this endpoint!", token);
      LOG.error("Authentication failed with 401 due to error: {}", refused.body());
      throw refused;
    }
  }
}
