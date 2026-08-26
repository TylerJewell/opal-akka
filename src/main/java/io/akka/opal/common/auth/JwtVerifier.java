package io.akka.opal.common.auth;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.akka.opal.common.auth.Types.JWTAlgorithm;
import java.text.ParseException;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies OPAL's own tokens — SPEC-002 R70 and R71.
 *
 * <p>Without a public key, verification is disabled and every caller is treated as allowed;
 * that is what lets OPAL run with no security at all for local development, and every
 * authorisation check in the product is written to return early in that state.
 */
public class JwtVerifier {

  private static final Logger log = LoggerFactory.getLogger(JwtVerifier.class);

  private final JWTAlgorithm algorithm;
  private final String audience;
  private final String issuer;
  private JWK jwk;
  private boolean enabled = true;

  public JwtVerifier(
      String publicKeyText,
      Types.EncryptionKeyFormat publicFormat,
      JWTAlgorithm algorithm,
      String audience,
      String issuer) {
    this.algorithm = algorithm;
    this.audience = audience;
    this.issuer = issuer;
    if (publicKeyText == null) {
      disable();
      return;
    }
    try {
      this.jwk = Keys.parsePublic(publicKeyText, publicFormat, algorithm);
    } catch (Exception e) {
      log.error("Invalid public key for jwt verification, error: {}!", e.toString());
      disable();
    }
  }

  protected void disable() {
    this.enabled = false;
  }

  public boolean enabled() {
    return enabled;
  }

  public JWTAlgorithm algorithm() {
    return algorithm;
  }

  public String audience() {
    return audience;
  }

  public String issuer() {
    return issuer;
  }

  public JWK jwk() {
    return jwk;
  }

  /** R74: the public half, as a JWK document. */
  public String getJwk() {
    if (jwk == null) {
      throw new IllegalStateException("no public key configured");
    }
    return jwk.toPublicJWK().toJSONString();
  }

  /**
   * R71: each failure has its own reason, and they are distinguishable because a caller acts on
   * them differently — an expired token is refreshed, a wrong audience is a misconfiguration.
   */
  public Map<String, Object> verify(String token) {
    try {
      SignedJWT jwt = SignedJWT.parse(token);
      if (!jwt.verify(JwtSigner.verifierFor(jwk))) {
        throw new Unauthorized("Could not decode access token", token);
      }
      JWTClaimsSet claims = jwt.getJWTClaimsSet();
      Date expiry = claims.getExpirationTime();
      if (expiry != null && expiry.toInstant().isBefore(Instant.now())) {
        throw new Unauthorized("Access token is expired", token);
      }
      if (audience != null && (claims.getAudience() == null
          || !claims.getAudience().contains(audience))) {
        throw new Unauthorized("Invalid access token: invalid audience claim", token);
      }
      if (issuer != null && !issuer.equals(claims.getIssuer())) {
        throw new Unauthorized("Invalid access token: invalid issuer claim", token);
      }
      Map<String, Object> verified = new LinkedHashMap<>(claims.getClaims());
      // The audience as the token carries it. The parser normalises a single audience into a
      // one-element list, and a caller reading this map gets the claim rather than the
      // parser's reading of it — the source hands back what was in the payload.
      Object rawAudience = rawClaim(token, "aud");
      if (rawAudience != null) {
        verified.put("aud", rawAudience);
      }
      return verified;
    } catch (Unauthorized e) {
      throw e;
    } catch (ParseException e) {
      throw new Unauthorized("Could not decode access token", token);
    } catch (Exception e) {
      throw new Unauthorized("Unknown JWT error", token);
    }
  }

  /** One claim, straight out of the payload, before any library normalised it. */
  private static Object rawClaim(String token, String name) {
    try {
      String[] parts = token.split("\\.");
      byte[] payload = java.util.Base64.getUrlDecoder().decode(parts[1]);
      com.fasterxml.jackson.databind.JsonNode json =
          io.akka.opal.server.pubsub.Rpc.MAPPER.readTree(payload);
      com.fasterxml.jackson.databind.JsonNode claim = json.get(name);
      if (claim == null || claim.isNull()) {
        return null;
      }
      return claim.isTextual()
          ? claim.asText()
          : io.akka.opal.server.pubsub.Rpc.MAPPER.convertValue(claim, Object.class);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * R70: on top of the signature, a token must carry a {@code sub} that parses as a UUID. A
   * token without one is refused even though its signature is good — the subject is the peer's
   * identity and every route that reads it expects an id rather than a name.
   */
  public Map<String, Object> verifyLoggedIn(String token) {
    if (!enabled) {
      log.debug("JWT verification disabled, cannot verify requests!");
      return Map.of();
    }
    if (token == null) {
      throw new Unauthorized("access token was not provided");
    }
    Map<String, Object> claims = verify(token);
    Object subject = claims.get("sub");
    if (subject == null || String.valueOf(subject).isEmpty() || !isUuid(String.valueOf(subject))) {
      throw new Unauthorized("invalid sub claim");
    }
    return claims;
  }

  /** OPAL's own subject is a UUID with the dashes removed, which Python's UUID() accepts. */
  static boolean isUuid(String text) {
    String hex = text.replace("-", "").replace("{", "").replace("}", "").replace("urn:uuid:", "");
    if (hex.length() != 32) {
      return false;
    }
    for (int i = 0; i < hex.length(); i++) {
      if (Character.digit(hex.charAt(i), 16) < 0) {
        return false;
      }
    }
    return true;
  }

  /** The bearer token in an Authorization header, or null. */
  public static String tokenFromHeader(String authorizationHeader) {
    if (authorizationHeader == null || authorizationHeader.isEmpty()) {
      return null;
    }
    int space = authorizationHeader.indexOf(' ');
    if (space < 0) {
      return null;
    }
    String scheme = authorizationHeader.substring(0, space);
    String token = authorizationHeader.substring(space + 1).trim();
    if (token.isEmpty() || !scheme.equalsIgnoreCase("bearer")) {
      return null;
    }
    return token;
  }
}
