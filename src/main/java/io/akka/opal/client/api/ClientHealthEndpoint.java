package io.akka.opal.client.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.client.ClientRuntime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The client's own health, and the two routes that force an update — SPEC-002 R91.
 *
 * <p>{@code /healthy} and {@code /ready} answer different questions. Ready is "has this client
 * ever finished loading", which is what a start-up gate wants; healthy is "did the last thing it
 * tried work, and is the engine still answering", which is what a load balancer wants. A client
 * that loaded once and then lost its engine is ready and not healthy.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class ClientHealthEndpoint extends AbstractHttpEndpoint {

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(ClientHealthEndpoint.class);

  private final ClientRuntime runtime;

  public ClientHealthEndpoint(ClientRuntime runtime) {
    this.runtime = runtime;
  }

  @Get("/healthy")
  public HttpResponse healthy() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      Map<String, Object> body = new LinkedHashMap<>();
      if (runtime.healthy()) {
        body.put("status", "ok");
        body.put("online", runtime.online());
        return Responses.ok(body);
      }
      if (runtime.offlineModeEnabled() && runtime.ready()) {
        body.put("status", "ok");
        body.put("online", false);
        return Responses.ok(body);
      }
      body.put("status", "unavailable");
      return Responses.json(StatusCodes.SERVICE_UNAVAILABLE, body);
    });
  }

  @Get("/ready")
  public HttpResponse ready() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      if (runtime.ready()) {
        return Responses.statusOk();
      }
      return Responses.json(StatusCodes.SERVICE_UNAVAILABLE, Map.of("status", "unavailable"));
    });
  }

  @Post("/policy-updater/trigger")
  public HttpResponse triggerPolicyUpdate() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      log.info("triggered policy update from api");
      runtime.policyUpdater().triggerUpdatePolicy(null, true);
      return Responses.statusOk();
    });
  }

  /** The peer type the client's own routes are for: a listener, not a policy engine. */
  private void requireListener() {
    java.util.Map<String, Object> claims =
        io.akka.opal.server.api.Authn.requireLoggedIn(runtime.verifier(), requestContext());
    io.akka.opal.common.auth.Authz.requirePeerType(
        runtime.verifier().enabled(),
        claims,
        io.akka.opal.common.schemas.Security.PeerType.listener);
  }

  /** R255: a client whose data updater is off has nothing to trigger, and says so. */
  @Post("/data-updater/trigger")
  public HttpResponse triggerPolicyDataUpdate() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      log.info("triggered policy data update from api");
      if (!Boolean.TRUE.equals(runtime.config().get("DATA_UPDATER_ENABLED"))) {
        return Responses.detail(
            StatusCodes.SERVICE_UNAVAILABLE,
            "Data Updater is currently disabled. Dynamic data updates are not available.");
      }
      // The reason is not internal: it travels on the update, is logged by the fetch, and is
      // echoed in the line the client writes when it applies one.
      runtime.fetchBaseDataConfiguration("request from sdk");
      return Responses.statusOk();
    });
  }

  /**
   * R256: what this client's policy store is, for a peer allowed to ask.
   *
   * <p>The answer carries the store's own bearer token unless the configuration excludes it, so
   * the route is behind the same listener-peer check its siblings are. Left open it hands that
   * token to anybody who can reach the client's port.
   */
  @Get("/policy-store/config")
  public HttpResponse getPolicyStoreDetails() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      return Responses.ok(runtime.policyStoreDetails());
    });
  }
}
