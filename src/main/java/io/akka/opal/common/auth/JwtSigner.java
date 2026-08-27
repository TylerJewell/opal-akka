package io.akka.opal.common.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.Ed25519Signer;
import com.nimbusds.jose.crypto.Ed25519Verifier;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetKeyPair;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.akka.opal.common.auth.Types.JWTAlgorithm;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Signs and verifies OPAL's own tokens — SPEC-002 R68 to R70.
 *
 * <p>With neither key configured, signing and verification are disabled and every request is
 * allowed; with exactly one, start-up fails; with both, they are checked against each other by
 * signing and verifying a probe token before anything else is accepted.
 */
public final class JwtSigner extends JwtVerifier {

  private static final Logger log = LoggerFactory.getLogger(JwtSigner.class);

  /** Raised when the two configured keys do not belong together. */
  public static final class InvalidJWTCryptoKeys extends RuntimeException {
    public InvalidJWTCryptoKeys(String message) {
      super(message);
    }
  }

  private final JWK privateJwk;

  public JwtSigner(
      String privateKeyText,
      String publicKeyText,
      Types.EncryptionKeyFormat privateFormat,
      Types.EncryptionKeyFormat publicFormat,
      String passphrase,
      JWTAlgorithm algorithm,
      String audience,
      String issuer) {
    super(publicKeyText, publicFormat, algorithm, audience, issuer);
    this.privateJwk =
        privateKeyText == null
            ? null
            : Keys.parsePrivate(privateKeyText, privateFormat, passphrase, algorithm);
    verifyCryptoKeys(privateKeyText, publicKeyText);
  }

  private void verifyCryptoKeys(String privateKeyText, String publicKeyText) {
    boolean hasPrivate = privateKeyText != null;
    boolean hasPublic = publicKeyText != null;
    if (hasPrivate && hasPublic) {
      try {
        String probe = signRaw(new JWTClaimsSet.Builder().claim("some", "payload").build());
        if (!verifySignature(probe)) {
          throw new InvalidJWTCryptoKeys("private key and public key do not match!");
        }
      } catch (InvalidJWTCryptoKeys e) {
        throw e;
      } catch (java.text.ParseException | com.nimbusds.jose.JOSEException e) {
        // R374: only a failure of the token itself reads as a mismatched pair. A key the library
        // could not load, or an algorithm it does not have, is a different fault and saying
        // "these two keys do not match" about it sends an operator to regenerate a good pair.
        log.info("JWT Signer key verification failed with error: {}", e.toString());
        throw new InvalidJWTCryptoKeys("private key and public key do not match!");
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else if (!hasPrivate && hasPublic) {
      throw new IllegalArgumentException(
          "JWT Signer not valid, you provided a public key without a private key!");
    } else if (hasPrivate) {
      throw new IllegalArgumentException(
          "JWT Signer not valid, you provided a private key without a public key!");
    } else {
      disable();
    }
  }

  /** R68: the five registered claims, the peer type, and anything else the caller asked for. */
  public String sign(String subjectHex, Duration lifetime, Map<String, Object> customClaims) {
    Instant issuedAt = Instant.now();
    JWTClaimsSet.Builder claims =
        new JWTClaimsSet.Builder()
            .issueTime(Date.from(issuedAt))
            .expirationTime(Date.from(issuedAt.plus(lifetime)))
            // A single audience is written as a string rather than as a one-element array:
            // the source signs it that way, and a token is compared as bytes by anything that
            // caches or logs it.
            .claim("aud", audience())
            .issuer(issuer())
            .subject(subjectHex);
    if (customClaims != null) {
      customClaims.forEach(claims::claim);
    }
    return signRaw(claims.build());
  }

  private String signRaw(JWTClaimsSet claims) {
    try {
      JWSHeader.Builder header = new JWSHeader.Builder(JWSAlgorithm.parse(algorithm().name()));
      String keyId = jwk() == null ? null : jwk().getKeyID();
      if (keyId != null) {
        header.keyID(keyId);
      }
      SignedJWT jwt = new SignedJWT(header.build(), claims);
      jwt.sign(signerFor(privateJwk));
      return jwt.serialize();
    } catch (JOSEException e) {
      throw new IllegalStateException("could not sign token", e);
    }
  }

  private boolean verifySignature(String token) throws Exception {
    SignedJWT jwt = SignedJWT.parse(token);
    return jwt.verify(verifierFor(jwk()));
  }

  static JWSSigner signerFor(JWK key) throws JOSEException {
    if (key instanceof RSAKey rsa) {
      return new RSASSASigner(rsa);
    }
    if (key instanceof ECKey ec) {
      return new ECDSASigner(ec);
    }
    if (key instanceof OctetKeyPair okp) {
      return new Ed25519Signer(okp);
    }
    if (key instanceof com.nimbusds.jose.jwk.OctetSequenceKey oct) {
      return new MACSigner(oct);
    }
    throw new JOSEException("unsupported key type for signing: " + key.getKeyType());
  }

  static JWSVerifier verifierFor(JWK key) throws JOSEException {
    if (key instanceof RSAKey rsa) {
      return new RSASSAVerifier(rsa.toPublicJWK());
    }
    if (key instanceof ECKey ec) {
      return new ECDSAVerifier(ec.toPublicJWK());
    }
    if (key instanceof OctetKeyPair okp) {
      return new Ed25519Verifier(okp.toPublicJWK());
    }
    if (key instanceof com.nimbusds.jose.jwk.OctetSequenceKey oct) {
      return new MACVerifier(oct);
    }
    throw new JOSEException("unsupported key type for verification: " + key.getKeyType());
  }
}
