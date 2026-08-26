package io.akka.opal.common.auth;

import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Security.PeerType;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The two claim checks OPAL's routes are guarded by — SPEC-002 R45, R63 and R73. */
public final class Authz {

  private Authz() {}

  /**
   * R73: with verification off nothing is checked at all; otherwise a missing, unparseable and
   * mismatched claim are three different refusals.
   */
  public static void requirePeerType(
      boolean verificationEnabled, Map<String, Object> claims, PeerType requiredType) {
    if (!verificationEnabled) {
      return;
    }
    Object peerType = claims.get("peer_type");
    if (peerType == null) {
      throw new Unauthorized("Missing 'peer_type' claim for OPAL jwt token");
    }
    PeerType type;
    try {
      type = PeerType.valueOf(String.valueOf(peerType));
    } catch (IllegalArgumentException e) {
      throw new Unauthorized("Invalid 'peer_type' claim for OPAL jwt token: " + peerType);
    }
    if (type != requiredType) {
      throw new Unauthorized(
          "Incorrect 'peer_type' claim for OPAL jwt token: PeerType."
              + type.name()
              + ", expected: PeerType."
              + requiredType.name());
    }
  }

  /**
   * R45: a token carrying {@code permitted_topics} may publish only inside that set. A token
   * without the claim is unrestricted, which is what makes the claim optional rather than a
   * second authorisation model.
   */
  public static void restrictOptionalTopicsToPublish(
      boolean verificationEnabled, Map<String, Object> claims, Data.DataUpdate update) {
    if (!verificationEnabled) {
      return;
    }
    if (!claims.containsKey("permitted_topics")) {
      return;
    }
    Set<String> permitted = new LinkedHashSet<>(asStrings(claims.get("permitted_topics")));
    for (Data.DataSourceEntry entry : update.entries()) {
      List<String> unauthorized = new ArrayList<>();
      for (String topic : entry.topics()) {
        if (!permitted.contains(topic)) {
          unauthorized.add(topic);
        }
      }
      if (!unauthorized.isEmpty()) {
        throw new Unauthorized(
            "Invalid 'topics' to publish " + io.akka.opal.common.util.Repr.pySet(unauthorized));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static List<String> asStrings(Object value) {
    List<String> out = new ArrayList<>();
    if (value instanceof List<?> list) {
      for (Object item : list) {
        out.add(String.valueOf(item));
      }
    } else if (value != null) {
      out.add(String.valueOf(value));
    }
    return out;
  }
}
