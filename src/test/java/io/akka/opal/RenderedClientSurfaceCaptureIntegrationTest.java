package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The client's two screens, as the running client serves them.
 *
 * <p>A separate class from the server's because the role is read once per process and the
 * document each role serves is the routes that role mounts — which is the whole of what the two
 * screens differ by.
 */
@ExtendWith(OpalProcessExtension.class)
public class RenderedClientSurfaceCaptureIntegrationTest extends TestKitSupport {

  static Path OUT;

  static void startProcess() {
    try {
      OUT = Path.of("..", "opal-port", "gui", "served").toAbsolutePath().normalize();
      Files.createDirectories(OUT);
      System.setProperty("OPAL_ROLE", "client");
      System.setProperty("OPAL_POLICY_STORE_TYPE", "MOCK");
      System.setProperty("OPAL_INLINE_OPA_ENABLED", "false");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterAll
  public static void clearProperties() {
    for (String name :
        List.of("OPAL_ROLE", "OPAL_POLICY_STORE_TYPE", "OPAL_INLINE_OPA_ENABLED")) {
      System.clearProperty(name);
    }
  }

  @Test
  public void theClientScreensAreWrittenAsServed() throws Exception {
    write("client-docs.html", "/docs");
    write("client-redoc.html", "/redoc");
    write("client-openapi.json", "/openapi.json");
    assertTrue(Files.size(OUT.resolve("client-docs.html")) > 0);
    assertTrue(Files.size(OUT.resolve("client-redoc.html")) > 0);
    assertTrue(Files.size(OUT.resolve("client-openapi.json")) > 0);
  }

  private void write(String name, String route) throws Exception {
    StrictResponse<ByteString> response = httpClient.GET(route).invoke();
    assertEquals(200, response.status().intValue(), route);
    Files.writeString(
        OUT.resolve(name),
        response.body().decodeString(StandardCharsets.UTF_8),
        StandardCharsets.UTF_8);
  }
}
