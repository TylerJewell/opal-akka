package io.akka.opal.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

/** Identity — SPEC-002 section 2.3. */
public final class Security {

  private Security() {}

  public enum PeerType {
    client,
    datasource,
    listener
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record AccessTokenRequest(
      String id, PeerType type, Object ttl, Map<String, Object> claims) {
    public AccessTokenRequest {
      if (type == null) {
        type = PeerType.client;
      }
      if (claims == null) {
        claims = Map.of();
      }
    }
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record TokenDetails(String id, PeerType type, String expired, Map<String, Object> claims) {}

  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record AccessToken(String token, String type, TokenDetails details) {
    public static AccessToken bearer(String token, TokenDetails details) {
      return new AccessToken(token, "bearer", details);
    }
  }
}
