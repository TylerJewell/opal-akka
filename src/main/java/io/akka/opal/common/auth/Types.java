package io.akka.opal.common.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** The enumerations OPAL's authentication configuration is drawn from. */
public final class Types {

  private Types() {}

  /** How a key is written down: PEM, OpenSSH (RFC4716 or the one-line form), or DER. */
  public enum EncryptionKeyFormat {
    pem,
    ssh,
    der;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static EncryptionKeyFormat from(String value) {
      return valueOf(value.toLowerCase());
    }
  }

  /**
   * The algorithms PyJWT registers by default, in its own order. OPAL builds its enumeration
   * from that registry, so the set is the library's rather than OPAL's own choice.
   */
  public enum JWTAlgorithm {
    none,
    HS256,
    HS384,
    HS512,
    RS256,
    RS384,
    RS512,
    ES256,
    ES256K,
    ES384,
    ES521,
    ES512,
    PS256,
    PS384,
    PS512,
    EdDSA;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static JWTAlgorithm from(String value) {
      for (JWTAlgorithm candidate : values()) {
        if (candidate.name().equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid JWTAlgorithm");
    }
  }
}
