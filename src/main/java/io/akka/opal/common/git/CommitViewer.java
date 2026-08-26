package io.akka.opal.common.git;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

/**
 * The repository as one commit sees it — SPEC-002 R27's traversal order.
 *
 * <p>The order is the directory itself, then that directory's own files, then each subdirectory
 * depth-first, which is the order a bundle's manifest is built in before an explicit manifest
 * reorders it. Git stores a tree's entries sorted by name with a directory compared as its name
 * plus a slash, so the order is a property of the repository rather than of this walk.
 */
public final class CommitViewer {

  /** A file or a directory at one commit. The root directory's path is {@code .}. */
  public record Node(String path, boolean directory, ObjectId id) {}

  private final Repository repository;
  private final RevCommit commit;
  private final List<Node> nodes = new ArrayList<>();
  private final Map<String, Node> byPath = new LinkedHashMap<>();

  public CommitViewer(Repository repository, ObjectId commitId) throws IOException {
    this.repository = repository;
    try (RevWalk walk = new RevWalk(repository)) {
      this.commit = walk.parseCommit(commitId);
    }
    walkTree(commit.getTree().getId(), ".");
  }

  public RevCommit commit() {
    return commit;
  }

  public String hash() {
    return commit.getName();
  }

  private void walkTree(ObjectId treeId, String path) throws IOException {
    record Child(String name, ObjectId id) {}
    add(new Node(path, true, treeId));
    List<Child> subtrees = new ArrayList<>();
    try (TreeWalk walk = new TreeWalk(repository)) {
      walk.addTree(treeId);
      walk.setRecursive(false);
      while (walk.next()) {
        String childPath = path.equals(".") ? walk.getNameString() : path + "/" + walk.getNameString();
        if (walk.isSubtree()) {
          subtrees.add(new Child(childPath, walk.getObjectId(0)));
        } else {
          add(new Node(childPath, false, walk.getObjectId(0)));
        }
      }
    }
    for (Child subtree : subtrees) {
      walkTree(subtree.id(), subtree.name());
    }
  }

  private void add(Node node) {
    nodes.add(node);
    byPath.putIfAbsent(node.path(), node);
  }

  public List<Node> nodes() {
    return nodes;
  }

  public List<Node> files() {
    return nodes.stream().filter(n -> !n.directory()).toList();
  }

  public List<Node> directories() {
    return nodes.stream().filter(Node::directory).toList();
  }

  public Optional<Node> getFile(String path) {
    Node node = byPath.get(io.akka.opal.common.util.PurePath.normalize(path));
    return node != null && !node.directory() ? Optional.of(node) : Optional.empty();
  }

  public Optional<Node> getDirectory(String path) {
    Node node = byPath.get(io.akka.opal.common.util.PurePath.normalize(path));
    return node != null && node.directory() ? Optional.of(node) : Optional.empty();
  }

  public Optional<Node> getNode(String path) {
    return Optional.ofNullable(byPath.get(io.akka.opal.common.util.PurePath.normalize(path)));
  }

  public boolean exists(String path) {
    return byPath.containsKey(io.akka.opal.common.util.PurePath.normalize(path));
  }

  public byte[] readBytes(Node node) throws IOException {
    return repository.open(node.id()).getBytes();
  }

  public String read(Node node) throws IOException {
    return new String(readBytes(node), StandardCharsets.UTF_8);
  }
}
