package io.akka.opal.server.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.git.BundleMaker;
import io.akka.opal.common.git.CommitViewer;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.util.PurePath;
import io.akka.opal.server.ServerRuntime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code GET /policy} — SPEC-002 R34 to R36 and OD-11.
 *
 * <p>With no {@code path} the whole repository; with paths, only those directories; with a
 * {@code base_hash} the repository holds, the difference between the two commits.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class BundlesEndpoint extends AbstractHttpEndpoint {

  private static final Logger log = LoggerFactory.getLogger(BundlesEndpoint.class);

  private final ServerRuntime runtime;

  public BundlesEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  @Get("/policy")
  public HttpResponse getPolicy() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(runtime.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }

      // R281: the clone is found on disk rather than read off a running watcher, so a process
      // that serves bundles without pulling them answers.
      Repository repository = runtime.repositoryOnDisk();
      // R36: two different 503s. No clone directory at all is "not found"; a clone that exists
      // and has no branch yet is "not ready", which is the window while the first clone runs.
      if (repository == null || !repository.getDirectory().exists()) {
        return Responses.detail(StatusCodes.SERVICE_UNAVAILABLE, "policy repo was not found");
      }

      List<String> paths = requestContext().queryParams().getAll("path");
      String baseHash = requestContext().queryParams().getString("base_hash").orElse(null);

      Set<String> directories = new LinkedHashSet<>();
      if (paths.isEmpty()) {
        directories.add(".");
      } else {
        for (String path : paths) {
          directories.add(PurePath.normalize(path.startsWith("/") ? path.substring(1) : path));
        }
      }

      try {
        ObjectId head = repository.resolve("HEAD");
        if (head == null) {
          return Responses.detail(StatusCodes.SERVICE_UNAVAILABLE, "policy repo is not ready");
        }
        if (!paths.isEmpty()) {
          CommitViewer viewer = new CommitViewer(repository, head);
          for (String directory : directories) {
            if (!viewer.exists(directory)) {
              return Responses.detail(
                  StatusCodes.NOT_FOUND,
                  "requested path " + directory + " was not found in the policy repo!");
            }
          }
        }
        BundleMaker maker = runtime.bundleMaker(repository, directories);
        if (baseHash == null) {
          return Responses.ok(maker.makeBundle(head));
        }
        ObjectId base = resolve(repository, baseHash);
        if (base == null) {
          // OD-11: the original answers 500 here, by two routes. A client that has lost its place
          // asks exactly this, and the only answer that lets it recover is the whole bundle.
          log.info(
              "base_hash '{}' is not in the policy repo; answering with a complete bundle",
              baseHash);
          return Responses.ok(maker.makeBundle(head));
        }
        return Responses.ok(maker.makeDiffBundle(base, head));
      } catch (Exception e) {
        log.error("could not build a policy bundle", e);
        return Responses.uncaught();
      }
    });
  }

  private static ObjectId resolve(Repository repository, String hash) {
    try {
      ObjectId id = repository.resolve(hash);
      if (id == null) {
        return null;
      }
      // resolve() answers for a hash the repository does not hold; reading it is what finds out.
      repository.parseCommit(id);
      return id;
    } catch (Exception e) {
      return null;
    }
  }
}
