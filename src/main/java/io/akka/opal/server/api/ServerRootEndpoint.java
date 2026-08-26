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
import io.akka.opal.client.ClientRuntime;
import io.akka.opal.server.ServerRuntime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code GET /}, {@code GET /healthcheck} and {@code GET /loadlimit} — SPEC-002 R119 and R128.
 *
 * <p>The first two are the one place the two roles collide: OPAL's server answers
 * {@code {"status":"ok"}} and its client answers its own health. Where both roles are enabled the
 * server's answer wins, because that is what a load balancer in front of a server expects, and
 * the client's own answer stays available at {@code /healthy}.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class ServerRootEndpoint extends AbstractHttpEndpoint {

  private final ServerRuntime server;
  private final ClientRuntime client;

  public ServerRootEndpoint(ServerRuntime server, ClientRuntime client) {
    this.server = server;
    this.client = client;
  }

  @Get("/")
  public HttpResponse root() {
    return Responses.guarded(requestContext(), () -> {
      if (Role.isServer()) {
        return Responses.statusOk();
      }
      return clientHealth();
    });
  }

  /**
   * R128 and R214: with the backbone health check on, a wedged backbone reader answers 503.
   *
   * <p>{@code GET /} says only that the process is serving. This says whether it is fit to be
   * given work: a replica whose backbone reader has given up still answers requests and will
   * never hear anything from its peers, so a load balancer should route away from it and a
   * supervisor should replace it. A reader part-way through an ordinary reconnection is fit, or
   * every blip would take the whole fleet out.
   */
  @Get("/healthcheck")
  public HttpResponse healthcheck() {
    return Responses.guarded(requestContext(), () -> {
      if (Role.isServer()) {
        if (!server.isBroadcasterHealthy()) {
          Map<String, Object> body = new LinkedHashMap<>();
          body.put("status", "error");
          body.put("broadcaster", "unhealthy");
          return Responses.json(StatusCodes.SERVICE_UNAVAILABLE, body);
        }
        return Responses.statusOk();
      }
      return clientHealth();
    });
  }

  private HttpResponse clientHealth() {
    if (client == null) {
      return Responses.notFound();
    }
    Map<String, Object> body = new LinkedHashMap<>();
    if (client.healthy()) {
      body.put("status", "ok");
      body.put("online", client.online());
      return Responses.ok(body);
    }
    if (client.offlineModeEnabled() && client.ready()) {
      body.put("status", "ok");
      body.put("online", false);
      return Responses.ok(body);
    }
    body.put("status", "unavailable");
    return Responses.json(StatusCodes.SERVICE_UNAVAILABLE, body);
  }

  /** R119: always 200, and rate-limited globally when a limit is configured. */
  @Get("/loadlimit")
  public HttpResponse loadlimit() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(server.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (!server.loadLimiter().allow()) {
        return Responses.detail(
            StatusCodes.TOO_MANY_REQUESTS,
            "Rate limit exceeded: " + server.config().getString("CLIENT_LOAD_LIMIT_NOTATION"));
      }
      return Responses.json(StatusCodes.OK, null);
    });
  }
}
