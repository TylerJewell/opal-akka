package io.akka.opal.common.confi;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A key configuration entry — SPEC-002 R8 and R9. The value is either a path to read or the key
 * text itself, and text carrying no newline has every underscore turned back into one, because
 * that is how a multi-line key survives an environment variable.
 *
 * <p>The rule is not applied to a public key in SSH format: that format is one line, so an
 * underscore in it is part of the key rather than a folded newline.
 */
public final class Keys {

  private Keys() {}

  public static String decode(String value, String keyFormat) {
    if (value == null) {
      return null;
    }
    String expanded = expandUser(value);
    // A key given as text is not a path, and asking the filesystem about it is not harmless: a
    // PEM block holds newlines, which some platforms reject outright rather than answering "no
    // such file". The question is asked only of something that could be a path at all.
    if (couldBeAPath(expanded)) {
      Path path = Path.of(expanded);
      if (Files.isRegularFile(path)) {
        try {
          return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
          throw new Confi.BadValue("could not read key file " + expanded + ": " + e.getMessage());
        }
      }
    }
    if ("ssh".equalsIgnoreCase(keyFormat) && isPublicOneLine(value)) {
      return value;
    }
    return maybeDecodeMultiline(value);
  }

  /** R8's transformation, on its own so a probe can run it against the source's. */
  public static String maybeDecodeMultiline(String key) {
    if (key.contains("\n")) {
      return key;
    }
    String decoded = key.replace('_', '\n');
    return decoded.endsWith("\n") ? decoded : decoded + "\n";
  }

  /** An OpenSSH public key is {@code <type> <base64> [comment]} — three fields on one line. */
  static boolean isPublicOneLine(String value) {
    return value.startsWith("ssh-") || value.startsWith("ecdsa-") || value.startsWith("sk-");
  }

  static boolean couldBeAPath(String value) {
    if (value.isEmpty() || value.length() > 4096) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\n' || c == '\r' || c == '<' || c == '>' || c == '|' || c == '"') {
        return false;
      }
    }
    try {
      Path.of(value);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  public static String expandUser(String value) {
    if (value.startsWith("~")) {
      return System.getProperty("user.home") + value.substring(1);
    }
    return value;
  }
}
