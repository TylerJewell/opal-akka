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
import io.akka.opal.server.ServerRuntime;
import java.util.Map;

/**
 * {@code GET /statistics} and {@code GET /stats} — SPEC-002 R112 and R118.
 *
 * <p>With statistics off both answer 501 rather than an empty document, and the message names the
 * variable that turns them on. An empty answer would be indistinguishable from a fleet with
 * nothing connected, which is the state an operator is usually checking for.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class StatisticsEndpoint extends AbstractHttpEndpoint {

  private static final String DISABLED =
      "This OPAL server does not have statistics turned on."
          + " To turn on, set this config var: OPAL_STATISTICS_ENABLED=true";

  private final ServerRuntime runtime;

  public StatisticsEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  @Get("/statistics")
  public HttpResponse getStatistics() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(runtime.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (runtime.statistics() == null) {
        return Responses.detail(StatusCodes.NOT_IMPLEMENTED, Map.of("error", DISABLED));
      }
      return Responses.ok(runtime.statistics().state());
    });
  }

  @Get("/stats")
  public HttpResponse getStatCounts() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(runtime.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (runtime.statistics() == null) {
        return Responses.detail(StatusCodes.NOT_IMPLEMENTED, Map.of("error", DISABLED));
      }
      return Responses.ok(runtime.statistics().stateBrief());
    });
  }
}
