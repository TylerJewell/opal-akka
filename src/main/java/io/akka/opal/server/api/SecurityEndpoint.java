package io.akka.opal.server.api;

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
import io.akka.opal.common.schemas.Security;
import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minting tokens and publishing the key that verifies them — SPEC-002 R72 and R74.
 *
 * <p>The JWKS document is written to disk at start-up and served from there, the way the source
 * mounts a static directory for it. That is not incidental: an operator can point a second
 * service at the same directory, and the file is the interface.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class SecurityEndpoint extends AbstractHttpEndpoint {

  private static final Logger log = LoggerFactory.getLogger(SecurityEndpoint.class);

  private final ServerRuntime runtime;

  public SecurityEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
    writeJwks();
  }

  /** R72: the master token as a static credential, and 503 when there is no signing key. */
  @Post("/token")
  public HttpResponse generateNewAccessToken(Security.AccessTokenRequest request) {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireMasterToken(
            runtime.config().getString("AUTH_MASTER_TOKEN"), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      if (!runtime.signer().enabled()) {
        return Responses.detail(
            StatusCodes.SERVICE_UNAVAILABLE,
            "opal server was not configured with security, cannot generate tokens!");
      }
      String id = request.id() == null ? UUID.randomUUID().toString() : request.id();
      Duration ttl = readTtl(request.ttl());
      Map<String, Object> claims = new LinkedHashMap<>();
      claims.put("peer_type", request.type().name());
      if (request.claims() != null) {
        claims.putAll(request.claims());
      }
      String token = runtime.signer().sign(id.replace("-", ""), ttl, claims);
      log.info("Generated opal token: peer_type={}", request.type().name());
      Security.TokenDetails details =
          new Security.TokenDetails(
              id, request.type(), Instant.now().plus(ttl).toString(), claims);
      return Responses.ok(Security.AccessToken.bearer(token, details));
    });
  }

  /** R74: the public half, as the framework serves it from the static directory. */
  @Get("/.well-known/jwks.json")
  public HttpResponse jwks() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Path file = jwksFile();
        if (Files.isRegularFile(file)) {
          return Responses.ok(Rpc.MAPPER.readTree(Files.readString(file, StandardCharsets.UTF_8)));
        }
      } catch (Exception e) {
        log.warn("could not read the jwks file: {}", e.toString());
      }
      return Responses.ok(Map.of());
    });
  }

  private Path jwksFile() {
    String url = runtime.config().getString("AUTH_JWKS_URL");
    String name = url.substring(url.lastIndexOf('/') + 1);
    return Path.of(runtime.config().getString("AUTH_JWKS_STATIC_DIR")).resolve(name);
  }

  private void writeJwks() {
    if (!Role.isServer()) {
      return;
    }
    try {
      Path file = jwksFile();
      Files.createDirectories(file.getParent());
      Object contents = Map.of();
      if (runtime.signer().enabled()) {
        contents =
            Map.of("keys", List.of(Rpc.MAPPER.readTree(runtime.signer().getJwk())));
      }
      Files.writeString(file, Rpc.MAPPER.writeValueAsString(contents), StandardCharsets.UTF_8);
    } catch (Exception e) {
      log.warn("could not write the jwks file: {}", e.toString());
    }
  }

  /**
   * A lifetime arrives as a number of seconds or as an ISO-8601 duration, which is what the
   * source's own type accepts, and what its command line sends for {@code --ttl 30 days}.
   */
  static Duration readTtl(Object ttl) {
    if (ttl == null) {
      return Duration.ofDays(365);
    }
    if (ttl instanceof Number number) {
      return Duration.ofMillis((long) (number.doubleValue() * 1000));
    }
    String text = String.valueOf(ttl);
    try {
      return Duration.ofMillis((long) (Double.parseDouble(text) * 1000));
    } catch (NumberFormatException ignored) {
      // Not a number of seconds; the other accepted form is an ISO-8601 duration.
    }
    try {
      return Duration.parse(text);
    } catch (Exception e) {
      return Duration.ofDays(365);
    }
  }
}
