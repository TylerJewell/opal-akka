package io.akka.opal.client.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Delete;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Authz;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Security.PeerType;
import io.akka.opal.client.ClientRuntime;
import io.akka.opal.server.api.Authn;
import java.util.Map;

/**
 * The callbacks register, over HTTP — SPEC-002 R97 and R98.
 *
 * <p>Registering the same URL and configuration under a key of the caller's choosing replaces the
 * automatically-keyed entry rather than adding a second, so a caller that wants a name for a
 * callback it already registered gets one rather than two calls per update.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/callbacks")
public class CallbacksEndpoint extends AbstractHttpEndpoint {

  private final ClientRuntime runtime;

  public CallbacksEndpoint(ClientRuntime runtime) {
    this.runtime = runtime;
  }

  private void requireListener() {
    Map<String, Object> claims = Authn.requireLoggedIn(runtime.verifier(), requestContext());
    Authz.requirePeerType(runtime.verifier().enabled(), claims, PeerType.listener);
  }

  @Get("/")
  public HttpResponse listCallbacks() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      return Responses.ok(runtime.callbacksRegister().all());
    });
  }

  @Get("/{key}")
  public HttpResponse getCallbackByKey(String key) {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      Data.CallbackEntry entry = runtime.callbacksRegister().get(key);
      if (entry == null) {
        return Responses.detail(StatusCodes.NOT_FOUND, "no callback found with this key");
      }
      return Responses.ok(entry);
    });
  }

  @Post("/")
  public HttpResponse registerCallback(Data.CallbackEntry entry) {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      String key = runtime.callbacksRegister().put(entry.url(), entry.config(), entry.key());
      return Responses.ok(runtime.callbacksRegister().get(key));
    });
  }

  @Delete("/{key}")
  public HttpResponse removeCallback(String key) {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isClient()) {
        return Responses.notFound();
      }
      try {
        requireListener();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (runtime.callbacksRegister().get(key) == null) {
        return Responses.detail(StatusCodes.NOT_FOUND, "no callback found with this key");
      }
      runtime.callbacksRegister().remove(key);
      return Responses.noContent();
    });
  }
}
