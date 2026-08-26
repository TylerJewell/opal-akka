package io.akka.opal.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.Role;
import io.akka.opal.server.pubsub.Rpc;
import java.io.InputStream;
import java.util.List;

/**
 * The OpenAPI document the two rendered screens draw — SPEC-002 R140 and section 6.
 *
 * <p>The descriptive half — title, description, version, and the schema components every route
 * refers to — is the source's own, vendored, because it is documentation of a protocol this
 * rebuild implements rather than of an implementation. The {@code paths} half is filtered to the
 * routes this deployment actually mounts, so a route that went missing shows up as a missing
 * entry rather than as a document that still promises it.
 */
public final class OpenApi {

  private OpenApi() {}

  public static JsonNode document(boolean scopesEnabled, boolean debugInternalStats) {
    ObjectNode base =
        (ObjectNode) read(Role.isServer() ? "/openapi/server.json" : "/openapi/client.json");
    ObjectNode declaredPaths = (ObjectNode) base.get("paths");
    ObjectNode paths = Rpc.MAPPER.createObjectNode();

    for (Routes.Route route : Routes.mounted(scopesEnabled, debugInternalStats)) {
      if (route.methods().isEmpty()) {
        continue;
      }
      String path = route.path();
      JsonNode described = declaredPaths.get(path);
      if (described == null) {
        continue;
      }
      ObjectNode entry =
          paths.has(path) ? (ObjectNode) paths.get(path) : paths.putObject(path);
      for (String method : route.methods()) {
        String verb = method.toLowerCase(java.util.Locale.ROOT);
        if (described.has(verb)) {
          entry.set(verb, described.get(verb));
        }
      }
    }
    base.set("paths", paths);
    return base;
  }

  /** The paths the document ends up listing, which is what the census test compares. */
  public static List<String> paths(JsonNode document) {
    List<String> out = new java.util.ArrayList<>();
    document.get("paths").fieldNames().forEachRemaining(out::add);
    java.util.Collections.sort(out);
    return out;
  }

  private static JsonNode read(String resource) {
    try (InputStream in = OpenApi.class.getResourceAsStream(resource)) {
      return Rpc.MAPPER.readTree(in);
    } catch (Exception e) {
      throw new IllegalStateException("could not read " + resource, e);
    }
  }
}
