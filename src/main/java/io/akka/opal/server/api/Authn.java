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

  public static Map<String, Object> requireLoggedIn(JwtVerifier verifier, RequestContext context) {
    return verifier.verifyLoggedIn(bearerToken(context));
  }

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
      throw new Unauthorized("unauthorized to access this endpoint!", token);
    }
  }
}
