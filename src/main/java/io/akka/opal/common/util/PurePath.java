package io.akka.opal.common.util;

import java.util.ArrayList;
import java.util.List;

/**
 * The subset of POSIX path semantics OPAL's bundle machinery depends on, reproduced so that
 * bundle paths, manifests and topics are the same strings on both systems.
 *
 * <p>Normalisation drops empty and {@code .} components and keeps {@code ..}, so {@code a//b}
 * and {@code ./a/b} are both {@code a/b} while {@code a/../b} is left alone. The empty path and
 * {@code .} are the same path. Ordering is the ordering of the normalised string, which is what
 * a sorted set of these paths is sorted by.
 */
public final class PurePath {

  private PurePath() {}

  public static String normalize(String path) {
    if (path == null) {
      return ".";
    }
    boolean absolute = path.startsWith("/");
    List<String> parts = new ArrayList<>();
    for (String part : path.split("/", -1)) {
      if (!part.isEmpty() && !part.equals(".")) {
        parts.add(part);
      }
    }
    if (parts.isEmpty()) {
      return absolute ? "/" : ".";
    }
    return (absolute ? "/" : "") + String.join("/", parts);
  }

  /** The parent of a path, or {@code .} for a one-component relative path. */
  public static String parent(String path) {
    String p = normalize(path);
    if (p.equals(".") || p.equals("/")) {
      return p;
    }
    int slash = p.lastIndexOf('/');
    if (slash < 0) {
      return ".";
    }
    if (slash == 0) {
      return "/";
    }
    return p.substring(0, slash);
  }

  /** Every ancestor, nearest first, ending at {@code .} (or {@code /}). Empty for a root. */
  public static List<String> parents(String path) {
    List<String> out = new ArrayList<>();
    String p = normalize(path);
    if (p.equals(".") || p.equals("/")) {
      return out;
    }
    String current = p;
    while (true) {
      String parent = parent(current);
      out.add(parent);
      if (parent.equals(".") || parent.equals("/")) {
        return out;
      }
      current = parent;
    }
  }

  /** The final component: {@code a/b.rego} is {@code b.rego}, a root has none. */
  public static String name(String path) {
    String p = normalize(path);
    if (p.equals(".") || p.equals("/")) {
      return "";
    }
    int slash = p.lastIndexOf('/');
    return slash < 0 ? p : p.substring(slash + 1);
  }

  /** The suffix including its dot, or the empty string. A leading dot is not a suffix. */
  public static String suffix(String path) {
    String name = name(path);
    int dot = name.lastIndexOf('.');
    if (dot <= 0) {
      return "";
    }
    return name.substring(dot);
  }

  public static String join(String parent, String child) {
    String p = normalize(parent);
    if (p.equals(".")) {
      return normalize(child);
    }
    if (p.equals("/")) {
      return normalize("/" + child);
    }
    return normalize(p + "/" + child);
  }

  /** Collapses {@code ..} against the component before it, the way a filesystem resolve does. */
  public static String resolveDots(String path) {
    boolean absolute = isAbsolute(path);
    java.util.ArrayDeque<String> parts = new java.util.ArrayDeque<>();
    for (String part : normalize(path).split("/", -1)) {
      if (part.isEmpty() || part.equals(".")) {
        continue;
      }
      if (part.equals("..")) {
        if (!parts.isEmpty() && !parts.peekLast().equals("..")) {
          parts.pollLast();
        } else if (!absolute) {
          parts.addLast(part);
        }
        continue;
      }
      parts.addLast(part);
    }
    if (parts.isEmpty()) {
      return absolute ? "/" : ".";
    }
    return (absolute ? "/" : "") + String.join("/", parts);
  }

  public static boolean isAbsolute(String path) {
    return path != null && path.startsWith("/");
  }
}
