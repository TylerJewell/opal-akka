package io.akka.opal.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** The two digests OPAL takes: of a fetched value, and of a downloaded file. */
public final class Hashing {

  private Hashing() {}

  public static String sha256(byte[] bytes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return hex(digest.digest(bytes));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String sha256(String text) {
    return sha256(text.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * SPEC-002 R52: a string is hashed as itself, and anything else as JSON written the way
   * {@link PythonJson} writes it — the digest travels in a data-entry report and is compared
   * across a fleet, so the bytes hashed have to be the same bytes on both systems.
   */
  public static String calcHash(Object value) {
    if (value == null) {
      return sha256("null");
    }
    if (value instanceof String s) {
      return sha256(s);
    }
    // A fetched value arrives as parsed JSON, and a JSON string is a string: hashing its
    // quoted form would give a different digest from the one the source records for the same
    // fetch, and the digest is what a fleet compares.
    if (value instanceof com.fasterxml.jackson.databind.JsonNode node && node.isTextual()) {
      return sha256(node.asText());
    }
    try {
      return sha256(PythonJson.dumps(value));
    } catch (Exception e) {
      return "";
    }
  }

  public static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
