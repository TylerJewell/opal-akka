package io.akka.opal.common.git;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.RenameDetector;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.util.io.DisabledOutputStream;

/**
 * What changed between two commits.
 *
 * <p>A rename counts twice: the new name is an addition and the old name is a deletion, which is
 * what lets a renamed policy be written before the old one is deleted (R78).
 */
public final class DiffViewer implements AutoCloseable {

  /** One changed path. {@code oldPath} is absent on an addition, {@code newPath} on a deletion. */
  public record Change(DiffEntry.ChangeType type, String oldPath, String newPath, ObjectId oldId,
      ObjectId newId) {

    public String aPath() {
      return type == DiffEntry.ChangeType.ADD ? null : oldPath;
    }

    public String bPath() {
      return type == DiffEntry.ChangeType.DELETE ? null : newPath;
    }
  }

  private final Repository repository;
  private final RevCommit oldCommit;
  private final RevCommit newCommit;
  private final List<Change> changes = new ArrayList<>();

  public DiffViewer(Repository repository, ObjectId oldId, ObjectId newId) throws IOException {
    this.repository = repository;
    try (RevWalk walk = new RevWalk(repository)) {
      this.oldCommit = walk.parseCommit(oldId);
      this.newCommit = walk.parseCommit(newId);
    }
    try (DiffFormatter formatter = new DiffFormatter(DisabledOutputStream.INSTANCE);
        var reader = repository.newObjectReader()) {
      formatter.setRepository(repository);
      formatter.setDetectRenames(true);
      RenameDetector detector = formatter.getRenameDetector();
      if (detector != null) {
        detector.setRenameLimit(4000);
      }
      CanonicalTreeParser oldTree = new CanonicalTreeParser();
      oldTree.reset(reader, oldCommit.getTree());
      CanonicalTreeParser newTree = new CanonicalTreeParser();
      newTree.reset(reader, newCommit.getTree());
      for (DiffEntry entry : formatter.scan(oldTree, newTree)) {
        changes.add(
            new Change(
                entry.getChangeType(),
                entry.getOldPath().equals(DiffEntry.DEV_NULL) ? null : entry.getOldPath(),
                entry.getNewPath().equals(DiffEntry.DEV_NULL) ? null : entry.getNewPath(),
                entry.getOldId() == null ? null : entry.getOldId().toObjectId(),
                entry.getNewId() == null ? null : entry.getNewId().toObjectId()));
      }
    }
  }

  public List<Change> changes() {
    return changes;
  }

  /** The new version of everything added, renamed-to, or modified. */
  public List<Change> addedOrModified() {
    List<Change> out = new ArrayList<>();
    for (Change change : changes) {
      if (change.type() == DiffEntry.ChangeType.ADD || change.type() == DiffEntry.ChangeType.RENAME) {
        out.add(change);
      }
    }
    for (Change change : changes) {
      if (change.type() == DiffEntry.ChangeType.MODIFY) {
        out.add(change);
      }
    }
    return out;
  }

  /** The old version of everything deleted or renamed-from. */
  public List<Change> deleted() {
    List<Change> out = new ArrayList<>();
    for (Change change : changes) {
      if (change.type() == DiffEntry.ChangeType.DELETE) {
        out.add(change);
      }
    }
    for (Change change : changes) {
      if (change.type() == DiffEntry.ChangeType.RENAME) {
        out.add(change);
      }
    }
    return out;
  }

  /** Every path either side of the diff names, files only. */
  public Set<String> affectedPaths() {
    Set<String> paths = new LinkedHashSet<>();
    for (Change change : changes) {
      if (change.aPath() != null) {
        paths.add(change.aPath());
      }
      if (change.bPath() != null) {
        paths.add(change.bPath());
      }
    }
    return paths;
  }

  public String readNew(Change change) throws IOException {
    return new String(repository.open(change.newId()).getBytes(), StandardCharsets.UTF_8);
  }

  public String readOld(Change change) throws IOException {
    return new String(repository.open(change.oldId()).getBytes(), StandardCharsets.UTF_8);
  }

  @Override
  public void close() {}
}
