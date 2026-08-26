package io.akka.opal.common.git;

import io.akka.opal.common.rego.Rego;
import io.akka.opal.common.monitoring.Apm;
import io.akka.opal.common.monitoring.Span;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.util.Glob;
import io.akka.opal.common.util.Paths2;
import io.akka.opal.common.util.PurePath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the two kinds of policy bundle — SPEC-002 R23 to R33.
 *
 * <p>A complete bundle is the repository at one commit. A differential bundle is what changed
 * between two, plus the list of what was deleted, which the client applies as writes and deletes
 * rather than as a replacement.
 */
public final class BundleMaker {

  private static final Logger log = LoggerFactory.getLogger(BundleMaker.class);

  private final Repository repository;
  private final Set<String> directories;
  private final List<String> extensions;
  private final String rootManifestPath;
  private final List<String> bundleIgnore;
  private final List<String> policyExtensions;

  public BundleMaker(
      Repository repository,
      Set<String> directories,
      List<String> extensions,
      String rootManifestPath,
      List<String> bundleIgnore,
      List<String> policyExtensions) {
    this.repository = repository;
    this.directories = directories;
    this.extensions = extensions;
    this.rootManifestPath = rootManifestPath == null ? ".manifest" : rootManifestPath;
    this.bundleIgnore = bundleIgnore;
    this.policyExtensions = policyExtensions;
  }

  private boolean hasExtension(String path) {
    return extensions == null || extensions.contains(PurePath.suffix(path));
  }

  private boolean isUnderDirectories(String path) {
    return Paths2.isChildOfDirectories(path, directories);
  }

  private String findIgnoreMatch(String path) {
    return bundleIgnore == null ? null : Glob.globStyleMatchPathToList(path, bundleIgnore);
  }

  private boolean keep(String path) {
    return hasExtension(path) && isUnderDirectories(path) && findIgnoreMatch(path) == null;
  }

  /**
   * R28 and R29: the explicit order, compiled from a {@code .manifest} file. An entry naming a
   * directory is expanded by reading that directory's own manifest; an entry that is absolute,
   * escapes its directory, does not exist, is ignored, or has already been visited is skipped.
   */
  public List<String> explicitManifest(CommitViewer viewer) {
    List<String> visited = new ArrayList<>();
    var root = viewer.getNode(rootManifestPath);
    if (root.isEmpty()) {
      log.info(
          "Root manifest path doesn't exist, no explicit order would be imposed on policy bundle");
      return List.of();
    }
    CommitViewer.Node node = root.get();
    if (!node.directory()) {
      String directory = PurePath.parent(node.path());
      return compileManifest(viewer, directory, PurePath.name(node.path()), visited);
    }
    return compileManifest(viewer, node.path(), ".manifest", visited);
  }

  private List<String> compileManifest(
      CommitViewer viewer, String directory, String manifestFileName, List<String> visited) {
    List<String> explicit = new ArrayList<>();
    String manifestPath = PurePath.join(directory, manifestFileName);
    try {
      var manifestFile = viewer.getFile(manifestPath);
      if (manifestFile.isEmpty()) {
        log.info("Manifest file {} not found, assuming empty", manifestPath);
        return explicit;
      }
      for (String rawEntry : viewer.read(manifestFile.get()).split("\\R", -1)) {
        if (rawEntry.isEmpty()) {
          continue;
        }
        String entry = PurePath.join(directory, rawEntry);
        if (PurePath.isAbsolute(rawEntry) || !escapesNothing(directory, entry)) {
          log.warn("  Path '{}' is outside current .manifest directory", entry);
          continue;
        }
        if (!viewer.exists(entry)) {
          log.warn("  Path '{}' does not exist", entry);
          continue;
        }
        String ignoreMatch = findIgnoreMatch(entry);
        if (ignoreMatch != null) {
          log.warn("  Path'{} is ignored by ignore glob '{}'", entry, ignoreMatch);
          continue;
        }
        if (visited.contains(entry)) {
          log.warn("  Path '{}' has redundant references", entry);
          continue;
        }
        visited.add(entry);
        if (viewer.getDirectory(entry).isPresent()) {
          explicit.addAll(compileManifest(viewer, entry, ".manifest", visited));
          continue;
        }
        explicit.add(entry);
      }
    } catch (Exception e) {
      log.warn("   Failed to compile manifest file '{}'", manifestPath, e);
      return List.of();
    }
    return explicit;
  }

  /**
   * True when {@code entry} really sits under {@code directory} rather than beside or above it.
   * The dots are collapsed first: a manifest line of {@code ../other/x.rego} joins to a path
   * whose unresolved parents still include the directory it is trying to leave.
   */
  private static boolean escapesNothing(String directory, String entry) {
    return PurePath.parents(PurePath.resolveDots(entry))
        .contains(PurePath.resolveDots(directory));
  }

  private List<String> sortManifest(List<String> unsorted, List<String> explicitSorting) {
    if (explicitSorting == null || explicitSorting.isEmpty()) {
      return unsorted;
    }
    return Paths2.sortAccordingToExplicitSorting(unsorted, explicitSorting);
  }

  /** R23, R24, R27: the repository at one commit. */
  public Policy.PolicyBundle makeBundle(ObjectId commit) throws IOException {
    CommitViewer viewer = new CommitViewer(repository, commit);
    List<Policy.DataModule> dataModules = new ArrayList<>();
    List<Policy.RegoModule> policyModules = new ArrayList<>();
    List<String> manifest = new ArrayList<>();
    List<String> explicit = explicitManifest(viewer);

    for (CommitViewer.Node file : viewer.files()) {
      if (!keep(file.path())) {
        continue;
      }
      String contents;
      try (Span ignored = Apm.trace("bundle_maker.git_file_read", file.path())) {
        contents = viewer.read(file);
      }
      String path = file.path();
      if (Rego.isDataModule(path)) {
        dataModules.add(new Policy.DataModule(PurePath.parent(path), contents));
        manifest.add(path);
      } else if (Rego.isPolicyModule(path, policyExtensions)) {
        String packageName = Rego.getRegoPackage(contents);
        policyModules.add(
            new Policy.RegoModule(path, packageName == null ? "" : packageName, contents));
        manifest.add(path);
      }
    }
    return new Policy.PolicyBundle(
        sortManifest(manifest, explicit), viewer.hash(), null, dataModules, policyModules, null);
  }

  /** R31, R32, R33: what changed, and what was deleted, between two commits. */
  public Policy.PolicyBundle makeDiffBundle(ObjectId oldCommit, ObjectId newCommit)
      throws IOException {
    CommitViewer newViewer = new CommitViewer(repository, newCommit);
    List<String> explicit = explicitManifest(newViewer);

    List<Policy.DataModule> dataModules = new ArrayList<>();
    List<Policy.RegoModule> policyModules = new ArrayList<>();
    List<String> deletedDataModules = new ArrayList<>();
    List<String> deletedPolicyModules = new ArrayList<>();
    List<String> manifest = new ArrayList<>();

    try (DiffViewer viewer = new DiffViewer(repository, oldCommit, newCommit)) {
      for (DiffViewer.Change change : viewer.addedOrModified()) {
        if (!diffKeep(change)) {
          continue;
        }
        String path = change.bPath();
        String contents = viewer.readNew(change);
        if (Rego.isDataModule(path)) {
          dataModules.add(new Policy.DataModule(PurePath.parent(path), contents));
          manifest.add(path);
        } else if (Rego.isPolicyModule(path, policyExtensions)) {
          String packageName = Rego.getRegoPackage(contents);
          policyModules.add(
              new Policy.RegoModule(path, packageName == null ? "" : packageName, contents));
          manifest.add(path);
        }
      }
      for (DiffViewer.Change change : viewer.deleted()) {
        if (!diffKeep(change)) {
          continue;
        }
        String path = change.aPath();
        if (Rego.isDataModule(path)) {
          deletedDataModules.add(PurePath.parent(path));
        } else if (Rego.isPolicyModule(path, policyExtensions)) {
          deletedPolicyModules.add(path);
        }
      }

      Policy.DeletedFiles deletedFiles = null;
      if (!deletedDataModules.isEmpty() || !deletedPolicyModules.isEmpty()) {
        List<String> deletedPolicies = new ArrayList<>(sortManifest(deletedPolicyModules, explicit));
        Collections.reverse(deletedPolicies);
        deletedFiles = new Policy.DeletedFiles(deletedDataModules, deletedPolicies);
      }

      return new Policy.PolicyBundle(
          sortManifest(manifest, explicit),
          newViewer.hash(),
          new CommitViewer(repository, oldCommit).hash(),
          dataModules,
          policyModules,
          deletedFiles);
    }
  }

  /**
   * A renamed, added or deleted file passes the filter when <em>either</em> of its two paths
   * does — otherwise a policy moved into an unwatched directory would leave the old copy behind.
   */
  private boolean diffKeep(DiffViewer.Change change) {
    boolean extension = extensions == null;
    boolean directory = false;
    for (String path : new String[] {change.aPath(), change.bPath()}) {
      if (path == null) {
        continue;
      }
      if (extensions != null && extensions.contains(PurePath.suffix(path))) {
        extension = true;
      }
      if (Paths2.isChildOfDirectories(path, directories)) {
        directory = true;
      }
    }
    String ignorePath = change.bPath() != null ? change.bPath() : change.aPath();
    return extension && directory && findIgnoreMatch(ignorePath) == null;
  }
}
