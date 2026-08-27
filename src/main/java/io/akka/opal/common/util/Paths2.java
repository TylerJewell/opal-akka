package io.akka.opal.common.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** OPAL's {@code PathUtils}, in the order its own callers use it. */
public final class Paths2 {

  private Paths2() {}

  /**
   * Every parent directory of every path, sorted, {@code .} included. SPEC-002 R21.
   *
   * <p>Sorted as paths rather than as strings — see {@link #BY_COMPONENT}.
   */
  public static List<String> intermediateDirectories(List<String> paths) {
    Set<String> directories = new TreeSet<>(BY_COMPONENT);
    for (String path : paths) {
      directories.addAll(PurePath.parents(path));
    }
    return new ArrayList<>(directories);
  }

  /**
   * Path order: one component at a time, a prefix before what extends it.
   *
   * <p>Not string order. The separator sorts after the dot as a character, so {@code a.b} would
   * come before {@code a/c} in a plain comparison and after it here — and this order reaches a
   * caller as the sequence of {@code changed_directories} and of {@code policy:} topics on every
   * policy update.
   */
  public static final java.util.Comparator<String> BY_COMPONENT =
      (left, right) -> {
        List<String> a = components(left);
        List<String> b = components(right);
        for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
          int order = a.get(i).compareTo(b.get(i));
          if (order != 0) {
            return order;
          }
        }
        return Integer.compare(a.size(), b.size());
      };

  /** {@code .} has no components, which is what puts it first. */
  private static List<String> components(String path) {
    if (path == null || path.isEmpty() || path.equals(".")) {
      return List.of();
    }
    List<String> parts = new ArrayList<>();
    for (String part : path.split("/")) {
      if (!part.isEmpty() && !part.equals(".")) {
        parts.add(part);
      }
    }
    return parts;
  }

  public static boolean isChildOfDirectories(String path, Set<String> directories) {
    for (String parent : PurePath.parents(path)) {
      if (directories.contains(parent)) {
        return true;
      }
    }
    return false;
  }

  public static List<String> filterChildrenPathsOfDirectories(
      List<String> paths, Set<String> directories) {
    List<String> out = new ArrayList<>();
    for (String path : paths) {
      if (isChildOfDirectories(path, directories)) {
        out.add(path);
      }
    }
    return out;
  }

  /**
   * The directories of a set that no other member contains. A parent swallows its children, so
   * a set holding {@code .} collapses to {@code .} alone. SPEC-002 R18.
   */
  public static Set<String> nonIntersectingDirectories(List<String> paths) {
    Set<String> output = new LinkedHashSet<>();
    for (String raw : paths) {
      String candidate = PurePath.normalize(raw);
      List<String> candidateParents = PurePath.parents(candidate);
      boolean covered = false;
      for (String parent : candidateParents) {
        if (output.contains(parent)) {
          covered = true;
          break;
        }
      }
      if (covered) {
        continue;
      }
      for (String existing : new ArrayList<>(output)) {
        if (PurePath.parents(existing).contains(candidate)) {
          output.remove(existing);
        }
      }
      output.add(candidate);
    }
    return output;
  }

  /**
   * SPEC-002 R30: the paths the explicit order names, in that order, then everything else in
   * its original order. A named path that is absent contributes nothing.
   */
  public static List<String> sortAccordingToExplicitSorting(
      List<String> unsortedPaths, List<String> explicitSorting) {
    List<String> unsorted = new ArrayList<>();
    for (String p : unsortedPaths) {
      unsorted.add(PurePath.normalize(p));
    }
    List<String> sorted = new ArrayList<>();
    for (String raw : explicitSorting) {
      String path = PurePath.normalize(raw);
      int index = unsorted.indexOf(path);
      if (index >= 0) {
        sorted.add(unsorted.remove(index));
      }
    }
    sorted.addAll(unsorted);
    return sorted;
  }

  public static List<String> sortedListFromSet(Set<String> set) {
    return new ArrayList<>(new TreeSet<>(set));
  }
}
