package io.akka.opal.common.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** OPAL's {@code PathUtils}, in the order its own callers use it. */
public final class Paths2 {

  private Paths2() {}

  /** Every parent directory of every path, sorted, {@code .} included. SPEC-002 R21. */
  public static List<String> intermediateDirectories(List<String> paths) {
    Set<String> directories = new TreeSet<>();
    for (String path : paths) {
      directories.addAll(PurePath.parents(path));
    }
    return new ArrayList<>(directories);
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
