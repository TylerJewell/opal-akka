package io.akka.opal.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Path matching in the two shapes OPAL uses it: a shell-style pattern matched against a path's
 * trailing components, and the {@code /**} form that means "this path and everything under it".
 *
 * <p>A pattern with no separator matches on the file name alone, so {@code *.rego} matches
 * {@code a/x.rego} as well as {@code x.rego} — SPEC-002 R83.
 */
public final class Glob {

  private Glob() {}

  /**
   * SPEC-002 R83, and R82's positive half: the first pattern that matches, or null. Returning
   * the pattern rather than a boolean is what lets a caller name the rule that excluded a path.
   */
  public static String globStyleMatchPathToList(String path, List<String> matchPaths) {
    if (matchPaths == null) {
      return null;
    }
    for (String matchPath : matchPaths) {
      if (matchPath.equals("/") || matchPath.equals("/**")) {
        return matchPath;
      }
      if (matchPath.endsWith("/**")) {
        if ((path + "/").startsWith(matchPath.substring(0, matchPath.length() - 3) + "/")) {
          return matchPath;
        }
      } else if (matches(path, matchPath)) {
        return matchPath;
      }
    }
    return null;
  }

  /** A path matcher with the same rule as Python's {@code PurePath.match}. */
  public static boolean matches(String path, String pattern) {
    if (pattern.isEmpty()) {
      return false;
    }
    List<String> patternParts = components(pattern);
    List<String> pathParts = components(path);
    boolean anchored = pattern.startsWith("/");
    if (patternParts.isEmpty()) {
      return false;
    }
    if (anchored) {
      if (patternParts.size() != pathParts.size() || !path.startsWith("/")) {
        return false;
      }
    } else if (patternParts.size() > pathParts.size()) {
      return false;
    }
    int offset = pathParts.size() - patternParts.size();
    for (int i = 0; i < patternParts.size(); i++) {
      if (!fnmatch(pathParts.get(offset + i), patternParts.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static List<String> components(String path) {
    List<String> parts = new ArrayList<>();
    for (String part : path.split("/")) {
      if (!part.isEmpty() && !part.equals(".")) {
        parts.add(part);
      }
    }
    return parts;
  }

  /** One path component against one shell-style pattern: {@code *}, {@code ?} and {@code [..]}. */
  public static boolean fnmatch(String name, String pattern) {
    return Pattern.compile(translate(pattern)).matcher(name).matches();
  }

  static String translate(String pattern) {
    StringBuilder regex = new StringBuilder();
    int i = 0;
    while (i < pattern.length()) {
      char c = pattern.charAt(i++);
      switch (c) {
        case '*' -> regex.append(".*");
        case '?' -> regex.append('.');
        case '[' -> {
          int j = i;
          if (j < pattern.length() && (pattern.charAt(j) == '!' || pattern.charAt(j) == '^')) {
            j++;
          }
          if (j < pattern.length() && pattern.charAt(j) == ']') {
            j++;
          }
          while (j < pattern.length() && pattern.charAt(j) != ']') {
            j++;
          }
          if (j >= pattern.length()) {
            regex.append("\\[");
          } else {
            String set = pattern.substring(i, j).replace("\\", "\\\\");
            i = j + 1;
            if (set.startsWith("!")) {
              set = "^" + set.substring(1);
            }
            regex.append('[').append(set).append(']');
          }
        }
        default -> regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return regex.toString();
  }
}
