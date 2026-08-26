package io.akka.opal.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The enumerations OPAL's server and client configurations are drawn from.
 *
 * <p>Each carries a wire value distinct from its Java name where the two differ — {@code Git}
 * is written {@code GIT} and {@code AWS_S3} is written {@code AWS-S3}, and a deployment sets
 * the written form.
 */
public final class Enums {

  private Enums() {}

  public enum PolicySourceTypes {
    Git("GIT"),
    Api("API");

    private final String wire;

    PolicySourceTypes(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static PolicySourceTypes from(String value) {
      for (PolicySourceTypes candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value) || candidate.name().equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid PolicySourceTypes");
    }
  }

  public enum PolicyBundleServerType {
    HTTP("HTTP"),
    AWS_S3("AWS-S3");

    private final String wire;

    PolicyBundleServerType(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static PolicyBundleServerType from(String value) {
      for (PolicyBundleServerType candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value) || candidate.name().equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid PolicyBundleServerType");
    }
  }

  public enum ServerRole {
    Primary("primary"),
    Secondary("secondary");

    private final String wire;

    ServerRole(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static ServerRole from(String value) {
      for (ServerRole candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid ServerRole");
    }
  }

  public enum PolicyStoreTypes {
    OPA("OPA"),
    CEDAR("CEDAR"),
    MOCK("MOCK");

    private final String wire;

    PolicyStoreTypes(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static PolicyStoreTypes from(String value) {
      for (PolicyStoreTypes candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid PolicyStoreTypes");
    }
  }

  public enum PolicyStoreAuth {
    NONE("none"),
    TOKEN("token"),
    OAUTH("oauth"),
    TLS("tls");

    private final String wire;

    PolicyStoreAuth(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static PolicyStoreAuth from(String value) {
      for (PolicyStoreAuth candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid PolicyStoreAuth");
    }
  }

  /** How much of an inline engine's own log output is piped into OPAL's log. */
  public enum EngineLogFormat {
    NONE("none"),
    MINIMAL("minimal"),
    HTTP("http"),
    FULL("full");

    private final String wire;

    EngineLogFormat(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static EngineLogFormat from(String value) {
      for (EngineLogFormat candidate : values()) {
        if (candidate.wire.equalsIgnoreCase(value)) {
          return candidate;
        }
      }
      throw new IllegalArgumentException(value + " - is not a valid EngineLogFormat");
    }
  }

  /** Which of OPAL's two processes this deployment is. OD-10. */
  public enum OpalRole {
    server,
    client,
    both;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static OpalRole from(String value) {
      return valueOf(value.toLowerCase());
    }
  }
}
