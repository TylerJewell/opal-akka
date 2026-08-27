package io.akka.opal.server.api;

import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The JSON Web Key Set the server publishes — SPEC-002 R74 and R327.
 *
 * <p>Written to {@code AUTH_JWKS_STATIC_DIR} once, at start-up, and served at
 * {@code AUTH_JWKS_URL}. The source mounts the whole directory at that URL's parent when it
 * builds its routes, so both the file and the path it appears at follow the two entries.
 */
public final class Jwks {

  private static final Logger log = LoggerFactory.getLogger(Jwks.class);

  private Jwks() {}

  /** The path the document is served at, with a leading slash. */
  public static String mountedAt(ServerRuntime runtime) {
    String url = runtime.config().getString("AUTH_JWKS_URL");
    if (url == null || url.isEmpty()) {
      return "/.well-known/jwks.json";
    }
    return url.startsWith("/") ? url : "/" + url;
  }

  static Path file(ServerRuntime runtime) {
    String url = mountedAt(runtime);
    String name = url.substring(url.lastIndexOf('/') + 1);
    return Path.of(runtime.config().getString("AUTH_JWKS_STATIC_DIR")).resolve(name);
  }

  /** Reads what is on disk, or an empty document when there is nothing to read. */
  public static Object read(ServerRuntime runtime) {
    try {
      Path file = file(runtime);
      if (Files.isRegularFile(file)) {
        return Rpc.MAPPER.readTree(Files.readString(file, StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      log.warn("could not read the jwks file: {}", e.toString());
    }
    return Map.of();
  }

  /** Writes the document from the signer's public half. Called once, when the server starts. */
  public static void write(ServerRuntime runtime) {
    try {
      Path file = file(runtime);
      Files.createDirectories(file.getParent());
      Object contents = Map.of();
      if (runtime.signer().enabled()) {
        contents = Map.of("keys", List.of(Rpc.MAPPER.readTree(runtime.signer().getJwk())));
      }
      Files.writeString(file, Rpc.MAPPER.writeValueAsString(contents), StandardCharsets.UTF_8);
    } catch (Exception e) {
      log.warn("could not write the jwks file: {}", e.toString());
    }
  }
}
