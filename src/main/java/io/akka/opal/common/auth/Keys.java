package io.akka.opal.common.auth;

import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetKeyPair;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.util.Base64URL;
import io.akka.opal.common.auth.Types.EncryptionKeyFormat;
import io.akka.opal.common.auth.Types.JWTAlgorithm;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import org.bouncycastle.crypto.util.OpenSSHPrivateKeyUtil;
import org.bouncycastle.crypto.util.OpenSSHPublicKeyUtil;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/**
 * Reading a key in each of the three formats OPAL accepts — SPEC-002 R8 and R9.
 *
 * <p>OpenSSH is here because it is OPAL's <em>default</em> public-key format, and neither the
 * JDK nor a JOSE library reads it: a one-line {@code ssh-rsa AAAA...} public key and an
 * {@code OPENSSH PRIVATE KEY} block both need their own parsers.
 */
public final class Keys {

  static {
    if (java.security.Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      java.security.Security.addProvider(new BouncyCastleProvider());
    }
  }

  private Keys() {}

  public static JWK parsePublic(String text, EncryptionKeyFormat format, JWTAlgorithm algorithm) {
    String trimmed = text.strip();
    PublicKey key =
        switch (format) {
          case pem -> readPemPublic(trimmed);
          case der -> readDerPublic(trimmed, algorithm);
          case ssh -> readSshPublic(trimmed);
        };
    return toJwk(key, algorithm);
  }

  public static JWK parsePrivate(
      String text, EncryptionKeyFormat format, String passphrase, JWTAlgorithm algorithm) {
    String trimmed = text.strip();
    PrivateKey key =
        switch (format) {
          case pem -> readPemPrivate(trimmed, passphrase);
          case der -> readDerPrivate(trimmed, algorithm);
          case ssh -> readSshPrivate(trimmed, passphrase);
        };
    return toJwk(key, algorithm);
  }

  private static JWK toJwk(PublicKey key, JWTAlgorithm algorithm) {
    if (key instanceof RSAPublicKey rsa) {
      return new RSAKey.Builder(rsa).build();
    }
    if (key instanceof ECPublicKey ec) {
      return new ECKey.Builder(Curve.forECParameterSpec(ec.getParams()), ec).build();
    }
    throw new IllegalArgumentException("unsupported public key type: " + key.getAlgorithm());
  }

  private static JWK toJwk(PrivateKey key, JWTAlgorithm algorithm) {
    if (key instanceof RSAPrivateKey rsa) {
      return new RSAKey.Builder(rsaPublicFrom(rsa)).privateKey(rsa).build();
    }
    if (key instanceof ECPrivateKey ec) {
      throw new IllegalArgumentException(
          "an EC private key must be supplied with its public half; use PEM or SSH format");
    }
    throw new IllegalArgumentException("unsupported private key type: " + key.getAlgorithm());
  }

  private static RSAPublicKey rsaPublicFrom(RSAPrivateKey key) {
    try {
      if (key instanceof java.security.interfaces.RSAPrivateCrtKey crt) {
        return (RSAPublicKey)
            KeyFactory.getInstance("RSA")
                .generatePublic(
                    new java.security.spec.RSAPublicKeySpec(
                        crt.getModulus(), crt.getPublicExponent()));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("could not derive the public half of an RSA key", e);
    }
    throw new IllegalArgumentException("RSA private key carries no public exponent");
  }

  private static PublicKey readPemPublic(String text) {
    try (PEMParser parser = new PEMParser(new StringReader(text))) {
      Object object = parser.readObject();
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
      if (object instanceof SubjectPublicKeyInfo info) {
        return converter.getPublicKey(info);
      }
      if (object instanceof PEMKeyPair pair) {
        return converter.getKeyPair(pair).getPublic();
      }
      if (object instanceof org.bouncycastle.cert.X509CertificateHolder holder) {
        return converter.getPublicKey(holder.getSubjectPublicKeyInfo());
      }
      throw new IllegalArgumentException("not a PEM public key");
    } catch (Exception e) {
      throw new IllegalArgumentException("could not read PEM public key: " + e.getMessage(), e);
    }
  }

  private static PrivateKey readPemPrivate(String text, String passphrase) {
    try (PEMParser parser = new PEMParser(new StringReader(text))) {
      Object object = parser.readObject();
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
      if (object instanceof PEMEncryptedKeyPair encrypted) {
        PEMDecryptorProvider decryptor =
            new JcePEMDecryptorProviderBuilder().build(passphrase.toCharArray());
        return converter.getKeyPair(encrypted.decryptKeyPair(decryptor)).getPrivate();
      }
      if (object instanceof PKCS8EncryptedPrivateKeyInfo encrypted) {
        var decryptor =
            new JceOpenSSLPKCS8DecryptorProviderBuilder()
                .setProvider("BC")
                .build(passphrase.toCharArray());
        return converter.getPrivateKey(encrypted.decryptPrivateKeyInfo(decryptor));
      }
      if (object instanceof PEMKeyPair pair) {
        return converter.getKeyPair(pair).getPrivate();
      }
      if (object instanceof PrivateKeyInfo info) {
        return converter.getPrivateKey(info);
      }
      throw new IllegalArgumentException("not a PEM private key");
    } catch (Exception e) {
      throw new IllegalArgumentException("could not read PEM private key: " + e.getMessage(), e);
    }
  }

  private static PublicKey readDerPublic(String text, JWTAlgorithm algorithm) {
    try {
      byte[] der = Base64.getDecoder().decode(stripArmour(text));
      return KeyFactory.getInstance(keyAlgorithm(algorithm))
          .generatePublic(new X509EncodedKeySpec(der));
    } catch (Exception e) {
      throw new IllegalArgumentException("could not read DER public key: " + e.getMessage(), e);
    }
  }

  private static PrivateKey readDerPrivate(String text, JWTAlgorithm algorithm) {
    try {
      byte[] der = Base64.getDecoder().decode(stripArmour(text));
      return KeyFactory.getInstance(keyAlgorithm(algorithm))
          .generatePrivate(new PKCS8EncodedKeySpec(der));
    } catch (Exception e) {
      throw new IllegalArgumentException("could not read DER private key: " + e.getMessage(), e);
    }
  }

  private static String keyAlgorithm(JWTAlgorithm algorithm) {
    String name = algorithm.name();
    if (name.startsWith("RS") || name.startsWith("PS")) {
      return "RSA";
    }
    if (name.startsWith("ES")) {
      return "EC";
    }
    if (name.equals("EdDSA")) {
      return "Ed25519";
    }
    throw new IllegalArgumentException("no asymmetric key type for algorithm " + name);
  }

  private static String stripArmour(String text) {
    StringBuilder body = new StringBuilder();
    for (String line : text.split("\\R")) {
      if (!line.startsWith("-----")) {
        body.append(line.strip());
      }
    }
    return body.toString();
  }

  /** A one-line OpenSSH public key: {@code <type> <base64 blob> [comment]}. */
  private static PublicKey readSshPublic(String text) {
    try {
      String[] parts = text.split("\\s+");
      if (parts.length < 2) {
        throw new IllegalArgumentException("not an OpenSSH public key");
      }
      byte[] blob = Base64.getDecoder().decode(parts[1]);
      var parameters = OpenSSHPublicKeyUtil.parsePublicKey(blob);
      if (parameters instanceof org.bouncycastle.crypto.params.RSAKeyParameters rsa) {
        return KeyFactory.getInstance("RSA")
            .generatePublic(
                new java.security.spec.RSAPublicKeySpec(rsa.getModulus(), rsa.getExponent()));
      }
      if (parameters instanceof Ed25519PublicKeyParameters) {
        throw new IllegalArgumentException("Ed25519 public keys are handled as OKP, not X.509");
      }
      throw new IllegalArgumentException("unsupported OpenSSH public key type " + parts[0]);
    } catch (Exception e) {
      throw new IllegalArgumentException("could not read SSH public key: " + e.getMessage(), e);
    }
  }

  private static PrivateKey readSshPrivate(String text, String passphrase) {
    try {
      if (text.contains("OPENSSH PRIVATE KEY")) {
        byte[] blob = Base64.getDecoder().decode(stripArmour(text));
        var parameters = OpenSSHPrivateKeyUtil.parsePrivateKeyBlob(blob);
        if (parameters instanceof org.bouncycastle.crypto.params.RSAPrivateCrtKeyParameters rsa) {
          return KeyFactory.getInstance("RSA")
              .generatePrivate(
                  new java.security.spec.RSAPrivateCrtKeySpec(
                      rsa.getModulus(),
                      rsa.getPublicExponent(),
                      rsa.getExponent(),
                      rsa.getP(),
                      rsa.getQ(),
                      rsa.getDP(),
                      rsa.getDQ(),
                      rsa.getQInv()));
        }
        throw new IllegalArgumentException("unsupported OpenSSH private key type");
      }
      return readPemPrivate(text, passphrase);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("could not read SSH private key: " + e.getMessage(), e);
    }
  }

  /** An Ed25519 pair, kept separate because it is an OKP rather than an X.509 key. */
  public static OctetKeyPair ed25519(byte[] privateSeed, byte[] publicKey) {
    return new OctetKeyPair.Builder(Curve.Ed25519, Base64URL.encode(publicKey))
        .d(Base64URL.encode(privateSeed))
        .build();
  }

  /** A symmetric key for the HS family, which OPAL allows through its algorithm enumeration. */
  public static OctetSequenceKey secret(String text) {
    return new OctetSequenceKey.Builder(text.getBytes(StandardCharsets.UTF_8)).build();
  }
}
