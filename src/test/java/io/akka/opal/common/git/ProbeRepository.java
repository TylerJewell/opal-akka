package io.akka.opal.common.git;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;

/**
 * The two-commit policy repository the source's own probe built, rebuilt here so that a bundle
 * from this port can be compared against the bundle the source produced from the same tree.
 *
 * <p>The identities and timestamps are fixed because a commit hash is part of what a bundle
 * reports; the recorded answers redact the two hashes to {@code H1} and {@code H2} rather than
 * depending on them, and this keeps the tree the same either way.
 */
public final class ProbeRepository implements AutoCloseable {

  private static final PersonIdent WHO =
      new PersonIdent("probe", "p@e", java.time.Instant.parse("2020-01-01T00:00:00Z"),
          java.time.ZoneOffset.UTC);

  public final Path root;
  public final Git git;
  public final ObjectId first;
  public final ObjectId second;

  public ProbeRepository() throws Exception {
    root = Files.createTempDirectory("opal-probe-repo-");
    git = Git.init().setDirectory(root.toFile()).setInitialBranch("master").call();

    write("rbac.rego", "package rbac\n\nallow = true\n");
    write("data.json", "{\"top\": 1}\n");
    Files.createDirectories(root.resolve("envs"));
    write("envs/prod.rego", "package envs.prod\n\nx = 1\n");
    write("envs/data.json", "{\"env\": \"prod\"}\n");
    write("ignored.txt", "not a policy\n");
    write(".manifest", "envs\nrbac.rego\n");
    write("envs/.manifest", "prod.rego\n");
    git.add().addFilepattern(".").call();
    first = git.commit().setMessage("first").setAuthor(WHO).setCommitter(WHO).call().getId();

    write("rbac.rego", "package rbac\n\nallow = false\n");
    write("envs/dev.rego", "package envs.dev\n\ny = 2\n");
    Files.delete(root.resolve("envs/prod.rego"));
    git.add().addFilepattern(".").call();
    git.add().setUpdate(true).addFilepattern(".").call();
    second = git.commit().setMessage("second").setAuthor(WHO).setCommitter(WHO).call().getId();
  }

  public Repository repository() {
    return git.getRepository();
  }

  private void write(String path, String contents) throws IOException {
    Files.writeString(root.resolve(path), contents, StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws Exception {
    git.close();
    try (var walk = Files.walk(root)) {
      walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }
}
