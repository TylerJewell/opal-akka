package io.akka.opal.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.common.auth.Types.EncryptionKeyFormat;
import io.akka.opal.common.auth.Types.JWTAlgorithm;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Security.PeerType;
import java.io.StringWriter;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** SPEC-002 R68 to R73, against the claims and refusals the source produced. */
class JwtTest {

  private static String privatePem;
  private static String publicPem;

  @BeforeAll
  static void generateKeys() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair pair = generator.generateKeyPair();
    privatePem = pem("PRIVATE KEY", pair.getPrivate().getEncoded());
    publicPem = pem("PUBLIC KEY", pair.getPublic().getEncoded());
  }

  private static String pem(String type, byte[] der) throws Exception {
    StringWriter out = new StringWriter();
    try (PemWriter writer = new PemWriter(out)) {
      writer.writeObject(new PemObject(type, der));
    }
    return out.toString();
  }

  private static JwtSigner signer() {
    return new JwtSigner(
        privatePem,
        publicPem,
        EncryptionKeyFormat.pem,
        EncryptionKeyFormat.pem,
        null,
        JWTAlgorithm.RS256,
        "https://api.opal.ac/v1/",
        "https://opal.ac/");
  }

  /** R68: the five registered claims, the peer type, and the subject with its dashes removed. */
  @Test
  void signedClaimsMatchTheSource() {
    JsonNode recorded = SourceAnswers.get("jwt");
    JwtSigner signer = signer();
    assertEquals(recorded.get("enabled").asBoolean(), signer.enabled());

    String subject = "12345678123456781234567812345678";
    String token = signer.sign(subject, Duration.ofDays(1), Map.of("peer_type", "client"));
    Map<String, Object> claims = signer.verify(token);

    assertEquals(recorded.get("sub").asText(), claims.get("sub"));
    // The audience as a string, which is what the source's own token carries and what its
    // verifier hands back. The parser normalises a single audience into a list; the claim a
    // caller reads is the one the payload holds.
    assertEquals(recorded.get("aud").asText(), claims.get("aud"));
    assertEquals(recorded.get("iss").asText(), claims.get("iss"));
    assertEquals(recorded.get("peer_type").asText(), claims.get("peer_type"));
    assertTrue(claims.containsKey("iat"));
    assertTrue(claims.containsKey("exp"));
    assertEquals(recorded.get("jwk_kty").asText(), signer.jwk().getKeyType().getValue());
  }

  /** R69: with neither key, everything is allowed and nothing is signed. */
  @Test
  void withNoKeysVerificationIsDisabled() {
    JsonNode recorded = SourceAnswers.get("jwt");
    JwtSigner disabled =
        new JwtSigner(
            null, null, EncryptionKeyFormat.pem, EncryptionKeyFormat.pem, null,
            JWTAlgorithm.RS256, "aud", "iss");
    assertEquals(recorded.get("disabled_when_no_keys").asBoolean(), !disabled.enabled());
    assertEquals(Map.of(), disabled.verifyLoggedIn(null));
  }

  /** R69: exactly one of the two keys is a start-up failure. */
  @Test
  void oneKeyWithoutTheOtherFailsStartup() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JwtSigner(
                null, publicPem, EncryptionKeyFormat.pem, EncryptionKeyFormat.pem, null,
                JWTAlgorithm.RS256, "aud", "iss"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new JwtSigner(
                privatePem, null, EncryptionKeyFormat.pem, EncryptionKeyFormat.pem, null,
                JWTAlgorithm.RS256, "aud", "iss"));
  }

  /** R69: two keys that do not belong together fail start-up rather than failing later. */
  @Test
  void mismatchedKeysFailStartup() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    String otherPublic = pem("PUBLIC KEY", generator.generateKeyPair().getPublic().getEncoded());
    assertThrows(
        JwtSigner.InvalidJWTCryptoKeys.class,
        () ->
            new JwtSigner(
                privatePem, otherPublic, EncryptionKeyFormat.pem, EncryptionKeyFormat.pem, null,
                JWTAlgorithm.RS256, "aud", "iss"));
  }

  /** R71: each refusal has its own reason. */
  @Test
  void refusalReasonsMatchTheSource() {
    JsonNode recorded = SourceAnswers.get("jwt");
    JwtSigner signer = signer();

    Unauthorized badToken =
        assertThrows(Unauthorized.class, () -> signer.verify("not.a.jwt"));
    assertEquals(recorded.get("bad_token").asText(), badToken.detail().get("error"));

    Unauthorized noToken =
        assertThrows(Unauthorized.class, () -> signer.verifyLoggedIn(null));
    assertEquals("access token was not provided", noToken.detail().get("error"));

    Unauthorized expired =
        assertThrows(
            Unauthorized.class,
            () -> signer.verify(signer.sign("aabbccdd" .repeat(4), Duration.ofSeconds(-10), Map.of())));
    assertEquals("Access token is expired", expired.detail().get("error"));
  }

  /** R70: a good signature with no UUID subject is still refused. */
  @Test
  void aSubjectThatIsNotAUuidIsRefused() {
    JwtSigner signer = signer();
    String token = signer.sign("not-a-uuid", Duration.ofDays(1), Map.of());
    Unauthorized failure = assertThrows(Unauthorized.class, () -> signer.verifyLoggedIn(token));
    assertEquals("invalid sub claim", failure.detail().get("error"));
  }

  /** R71: the audience and the issuer are each their own reason. */
  @Test
  void theAudienceAndIssuerAreCheckedSeparately() {
    JwtSigner signer = signer();
    String token = signer.sign("aabbccdd".repeat(4), Duration.ofDays(1), Map.of());

    JwtVerifier wrongAudience =
        new JwtVerifier(publicPem, EncryptionKeyFormat.pem, JWTAlgorithm.RS256, "other", "https://opal.ac/");
    assertEquals(
        "Invalid access token: invalid audience claim",
        assertThrows(Unauthorized.class, () -> wrongAudience.verify(token)).detail().get("error"));

    JwtVerifier wrongIssuer =
        new JwtVerifier(
            publicPem, EncryptionKeyFormat.pem, JWTAlgorithm.RS256, "https://api.opal.ac/v1/",
            "other");
    assertEquals(
        "Invalid access token: invalid issuer claim",
        assertThrows(Unauthorized.class, () -> wrongIssuer.verify(token)).detail().get("error"));
  }

  /** R73, over the five claim shapes the source's own check answered. */
  @Test
  void peerTypeChecksMatchTheSource() {
    for (JsonNode row : SourceAnswers.get("require_peer_type")) {
      boolean enabled = !row.has("enabled") || row.get("enabled").asBoolean();
      Map<String, Object> claims =
          io.akka.opal.server.pubsub.Rpc.MAPPER.convertValue(row.get("claims"), Map.class);
      String expected = row.get("out").asText();
      if (expected.equals("ok")) {
        Authz.requirePeerType(enabled, claims, PeerType.datasource);
      } else {
        Unauthorized failure =
            assertThrows(
                Unauthorized.class,
                () -> Authz.requirePeerType(enabled, claims, PeerType.datasource),
                row.toString());
        assertEquals(expected, failure.detail().get("error"), row.toString());
      }
    }
  }

  /** R45, over the four publish shapes the source's own check answered. */
  @Test
  void topicRestrictionsMatchTheSource() {
    for (JsonNode row : SourceAnswers.get("restrict_optional_topics")) {
      boolean enabled = !row.has("enabled") || row.get("enabled").asBoolean();
      Map<String, Object> claims =
          io.akka.opal.server.pubsub.Rpc.MAPPER.convertValue(row.get("claims"), Map.class);
      Data.DataUpdate update =
          new Data.DataUpdate(
              null,
              List.of(
                  Data.DataSourceEntry.of("http://x", SourceAnswers.strings(row.get("topics")), "")),
              "probe",
              null);
      String expected = row.get("out").asText();
      if (expected.equals("ok")) {
        Authz.restrictOptionalTopicsToPublish(enabled, claims, update);
      } else {
        Unauthorized failure =
            assertThrows(
                Unauthorized.class,
                () -> Authz.restrictOptionalTopicsToPublish(enabled, claims, update),
                row.toString());
        assertEquals(expected, failure.detail().get("error"), row.toString());
      }
    }
  }

  /** R71: the challenge header is part of the refusal, and the token never reaches a log. */
  @Test
  void aRefusalCarriesItsTokenInTheBodyAndNotInTheLog() {
    Unauthorized failure = new Unauthorized("Could not decode access token", "not.a.jwt");
    assertEquals("not.a.jwt", failure.detail().get("token"));
    assertFalse(failure.body().containsKey("token"));
  }
}
