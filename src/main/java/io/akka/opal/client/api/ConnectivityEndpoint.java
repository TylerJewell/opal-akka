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
import io.akka.opal.client.ClientRuntime;
import io.akka.opal.common.auth.Authz;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.schemas.Security.PeerType;
import io.akka.opal.server.api.Authn;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turning the server connection off and on at runtime — SPEC-002 R99.
 *
 * <p>Both refuse unless offline mode is enabled, because disconnecting a client that cannot fall
 * back to a backup leaves it serving whatever happened to be in the engine, with no way to say
 * how old it is.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/opal-server")
public class ConnectivityEndpoint extends AbstractHttpEndpoint {

  private final ClientRuntime runtime;

  public ConnectivityEndpoint(ClientRuntime runtime) {
    this.runtime = runtime;
  }

  private void requireListener() {
    Map<String, Object> claims = Authn.requireLoggedIn(runtime.verifier(), requestContext());
    Authz.requirePeerType(runtime.verifier().enabled(), claims, PeerType.listener);
  }

  @Get("/connectivity")
  public HttpResponse getConnectivityStatus() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      Map<String, Object> body = new LinkedHashMap<>();
      body.put("opal_server_connectivity_disabled", runtime.connectivityDisabled());
      body.put("offline_mode_enabled", runtime.offlineModeEnabled());
      return Responses.ok(body);
    });
  }

  @Post("/connectivity/disable")
  public HttpResponse disableConnectivity() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (!runtime.offlineModeEnabled()) {
        return Responses.detail(
            StatusCodes.BAD_REQUEST,
            "Cannot disable OPAL server connectivity: offline mode is not enabled");
      }
      boolean changed = runtime.disableServerConnectivity();
      return Responses.ok(Map.of("status", changed ? "disabled" : "already_disabled"));
    });
  }

  @Post("/connectivity/enable")
  public HttpResponse enableConnectivity() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (!runtime.offlineModeEnabled()) {
        return Responses.detail(
            StatusCodes.BAD_REQUEST,
            "Cannot enable OPAL server connectivity: offline mode is not enabled");
      }
      boolean changed = runtime.enableServerConnectivity();
      return Responses.ok(Map.of("status", changed ? "enabled" : "already_enabled"));
    });
  }
}
