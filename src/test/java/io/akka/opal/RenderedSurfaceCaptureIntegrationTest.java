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
 * Writes the two rendered screens, exactly as the running service serves them, for the
 * appearance check in {@code RENDERING.md} R5.
 *
 * <p>The bytes are taken from a runtime that has started this service's own endpoints and
 * answered a real request for each page, which is what makes the capture a capture of the
 * rebuild. They are written to {@code opal-port/gui/served/} and the capture script renders them
 * from a static server on the machine, because this target's runtime does not stay up outside a
 * test or a deployment — the service is one process per test class here, and it ends with the
 * class.
 *
 * <p>What that costs is one variable: the browser is handed the same bytes by a different
 * process. Everything the comparison is about — the markup, the document it draws, the fonts and
 * the layout — is unchanged by that, and the alternative was a screenshot of nothing.
 *
 * <p>The role is the server's. {@code ClientEndpointIntegrationTest}'s own copy of this writes
 * the client's two, because the document differs by role and the manifest declares four screens.
 */
@ExtendWith(OpalProcessExtension.class)
public class RenderedSurfaceCaptureIntegrationTest extends TestKitSupport {

  static Path OUT;
  private static io.akka.opal.common.git.ProbeRepository REPO;

  static void startProcess() {
    try {
      OUT = Path.of("..", "opal-port", "gui", "served").toAbsolutePath().normalize();
      Files.createDirectories(OUT);
      // A real repository, because the watcher retries an unreachable one for as long as
      // anybody waits — which is R321, and which hangs a test that started it and then blocked.
      REPO = new io.akka.opal.common.git.ProbeRepository();
      System.setProperty("OPAL_ROLE", "server");
      System.setProperty("OPAL_STATISTICS_ENABLED", "true");
      System.setProperty("OPAL_POLICY_REPO_URL", REPO.root.toUri().toString());
      System.setProperty("OPAL_POLICY_REPO_MAIN_BRANCH", "master");
      // Its own clone directory. The default is one beside the project, which another test or
      // another run may already hold — and a clone path that cannot be prepared is retried for
      // as long as anybody waits.
      System.setProperty(
          "OPAL_POLICY_REPO_CLONE_PATH",
          Files.createTempDirectory("opal-capture-clone-").toString());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @AfterAll
  public static void clearProperties() throws Exception {
    REPO.close();
    for (String name :
        List.of(
            "OPAL_ROLE",
            "OPAL_STATISTICS_ENABLED",
            "OPAL_POLICY_REPO_URL",
            "OPAL_POLICY_REPO_MAIN_BRANCH",
            "OPAL_POLICY_REPO_CLONE_PATH")) {
      System.clearProperty(name);
    }
  }

  @Test
  public void theServerScreensAreWrittenAsServed() throws Exception {
    write("server-docs.html", "/docs");
    write("server-redoc.html", "/redoc");
    write("server-openapi.json", "/openapi.json");
    assertTrue(Files.size(OUT.resolve("server-docs.html")) > 0);
    assertTrue(Files.size(OUT.resolve("server-redoc.html")) > 0);
    assertTrue(Files.size(OUT.resolve("server-openapi.json")) > 0);
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
