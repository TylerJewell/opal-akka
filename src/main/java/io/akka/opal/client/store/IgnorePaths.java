package io.akka.opal.client.store;

import io.akka.opal.common.util.Glob;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code POLICY_STORE_POLICY_PATHS_TO_IGNORE} — SPEC-002 R82.
 *
 * <p>A pattern beginning with {@code !} is a negation and it is checked first, so it wins over
 * every positive pattern whatever order they were written in. That is what lets an operator
 * ignore a whole tree and keep one file out of it.
 */
public final class IgnorePaths {

  private IgnorePaths() {}

  public static boolean shouldIgnorePath(String path, List<String> ignorePaths) {
    if (ignorePaths == null || ignorePaths.isEmpty()) {
      return false;
    }
    List<String> toIgnore = new ArrayList<>();
    List<String> toNotIgnore = new ArrayList<>();
    for (String pattern : ignorePaths) {
      if (pattern.startsWith("!")) {
        toNotIgnore.add(pattern.substring(1));
      } else {
        toIgnore.add(pattern);
      }
    }
    if (Glob.globStyleMatchPathToList(path, toNotIgnore) != null) {
      return false;
    }
    return Glob.globStyleMatchPathToList(path, toIgnore) != null;
  }
}
