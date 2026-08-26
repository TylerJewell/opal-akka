package io.akka.opal.server.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.scopes.GitPolicyFetcher;

/**
 * The two debug routes — SPEC-002 R133 and OD-9.
 *
 * <p>The cache-stats route exists only with {@code DEBUG_INTERNAL_STATS} on, exactly as in the
 * source. The metrics route is this port's own: it is where the measurements OPAL sends to a
 * vendor agent are readable instead.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/internal")
public class InternalEndpoint extends AbstractHttpEndpoint {

  private final ServerRuntime runtime;

  public InternalEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  @Get("/git-fetcher-cache-stats")
  public HttpResponse gitFetcherCacheStats() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer() || !Boolean.TRUE.equals(runtime.config().get("DEBUG_INTERNAL_STATS"))) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(runtime.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      return Responses.ok(GitPolicyFetcher.cacheStats());
    });
  }

  /** OD-9: every measurement OPAL hands to a third party's agent, readable here instead. */
  @Get("/metrics")
  public HttpResponse metrics() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      return Responses.ok(runtime.metrics().snapshot());
    });
  }
}
