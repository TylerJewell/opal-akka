package io.akka.opal.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.client.ClientRuntime;
import io.akka.opal.server.ServerRuntime;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * The two screens OPAL renders — SPEC-002 R140, R141, and RENDERING.md R3.
 *
 * <p>These are the source's own HTML, vendored verbatim under {@code resources/gui/}: the same
 * shells, the same third-party bundles at the same URLs, the same titles. What changed is where
 * they get their data — the OpenAPI document is this rebuild's, built from the routes it actually
 * mounts. Nothing about the markup, the styling or the layout was rewritten, which is what makes
 * the appearance comparison against a baseline captured from the original mean anything.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class DocsEndpoint extends AbstractHttpEndpoint {

  private final ServerRuntime server;
  private final ClientRuntime client;

  public DocsEndpoint(ServerRuntime server, ClientRuntime client) {
    this.server = server;
    this.client = client;
  }

  private boolean scopesEnabled() {
    return server != null && server.scopesEnabled();
  }

  private boolean debugInternalStats() {
    return server != null && Boolean.TRUE.equals(server.config().get("DEBUG_INTERNAL_STATS"));
  }

  @Get("/openapi.json")
  public HttpResponse openapi() {
    return Responses.guarded(requestContext(), () -> {
      return Responses.ok(OpenApi.document(scopesEnabled(), debugInternalStats()));
    });
  }

  @Get("/docs")
  public HttpResponse swaggerUiHtml() {
    return Responses.guarded(requestContext(), () -> {
      return Responses.html(underRootPath(vendored("swagger-ui.html")));
    });
  }

  @Get("/docs/oauth2-redirect")
  public HttpResponse swaggerUiRedirect() {
    return Responses.guarded(requestContext(), () -> {
      return Responses.html(vendored("oauth2-redirect.html"));
    });
  }

  @Get("/redoc")
  public HttpResponse redocHtml() {
    return Responses.guarded(requestContext(), () -> {
      return Responses.html(underRootPath(vendored("redoc.html")));
    });
  }

  /**
   * R338: the document's address is written relative to the prefix this process is served under.
   *
   * <p>Behind a reverse proxy mounting the process at {@code /opal}, a page fetching
   * {@code /openapi.json} asks for a path the proxy does not map. The source reads the prefix
   * from its web server, which is given it on the command line; there is no such argument here,
   * so the prefix comes from the header a proxy sends to say what it stripped. With no proxy in
   * front there is no header, the prefix is empty, and the page is byte for byte what the source
   * serves.
   */
  private String underRootPath(String markup) {
    String prefix = rootPath();
    return prefix.isEmpty() ? markup : markup.replace("/openapi.json", prefix + "/openapi.json");
  }

  private String rootPath() {
    String prefix =
        requestContext()
            .requestHeader("x-forwarded-prefix")
            .map(header -> header.value())
            .orElse("");
    while (prefix.endsWith("/")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }
    return prefix;
  }

  /** The shell as the source serves it, with only the title's process name filled in. */
  private String vendored(String name) {
    String markup = read("/gui/" + name);
    return markup.replace("{{TITLE}}", Role.isServer() ? "Opal Server" : "OPAL Client");
  }

  private static String read(String resource) {
    try (InputStream in = DocsEndpoint.class.getResourceAsStream(resource)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException("could not read " + resource, e);
    }
  }
}
