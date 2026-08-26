package io.akka.opal.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import io.akka.opal.cli.OpalCli;
import io.akka.opal.server.LoadLimiter;
import io.akka.opal.server.pubsub.Rpc;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The surface each role offers — SPEC-002 R119, R127, R134 to R140.
 *
 * <p>The route census is compared against the table read off the running original rather than
 * against a description of it, because a route quietly missing and a route quietly added are the
 * two ways a rebuild stops being a replacement.
 */
class SurfaceTest {

  private static JsonNode routeCensus(String name) throws Exception {
    try (InputStream in = SurfaceTest.class.getResourceAsStream("/source/routes-" + name + ".json")) {
      return Rpc.MAPPER.readTree(in);
    }
  }

  private static List<String> describe(List<Routes.Route> routes) {
    List<String> out = new ArrayList<>();
    for (Routes.Route route : routes) {
      out.add(route.path() + " " + route.methods() + " " + route.name());
    }
    return out;
  }

  private static List<String> describeRecorded(JsonNode census) {
    List<String> out = new ArrayList<>();
    for (JsonNode route : census) {
      out.add(
          route.get("path").asText()
              + " "
              + SourceAnswers.strings(route.get("methods"))
              + " "
              + route.get("name").asText());
    }
    return out;
  }

  /** R127: the 19 the server mounts, in the source's own order. */
  @Test
  void theServerRouteTableMatchesTheSource() throws Exception {
    assertEquals(
        describeRecorded(routeCensus("server")), describe(Routes.serverRoutes(false, false)));
  }

  /** R127: the 28 with scopes on. */
  @Test
  void theScopedServerRouteTableMatchesTheSource() throws Exception {
    assertEquals(
        describeRecorded(routeCensus("server-scopes")), describe(Routes.serverRoutes(true, false)));
  }

  /** R127: the 18 the client mounts. */
  @Test
  void theClientRouteTableMatchesTheSource() throws Exception {
    assertEquals(describeRecorded(routeCensus("client")), describe(Routes.CLIENT));
  }

  /** R140: the OpenAPI document lists the paths the source's own document lists. */
  @Test
  void theOpenApiDocumentListsTheSourcePaths() throws Exception {
    JsonNode document = OpenApi.document(false, false);
    assertEquals(
        SourceAnswers.strings(SourceAnswers.get("openapi_paths")), OpenApi.paths(document));
    assertEquals("Opal Server", document.get("info").get("title").asText());
  }

  /** R141: the shells are the source's own, referring to the same bundles at the same URLs. */
  @Test
  void theRenderedShellsAreTheSourcesOwn() throws Exception {
    String swagger = read("/gui/swagger-ui.html");
    assertTrue(swagger.contains("https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"));
    assertTrue(
        swagger.contains("https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"));
    assertTrue(swagger.contains("url: '/openapi.json'"));

    String redoc = read("/gui/redoc.html");
    assertTrue(
        redoc.contains("https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js"));
    assertTrue(redoc.contains("<redoc spec-url=\"/openapi.json\"></redoc>"));
  }

  private static String read(String resource) throws Exception {
    try (InputStream in = SurfaceTest.class.getResourceAsStream(resource)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** R119: the limit is global, and beyond it the answer is a refusal. */
  @Test
  void theLoadLimitIsGlobal() {
    LoadLimiter limiter = new LoadLimiter("2 per minute");
    assertTrue(limiter.allow());
    assertTrue(limiter.allow());
    assertFalse(limiter.allow());
    assertFalse(limiter.allow());
  }

  @Test
  void noNotationMeansNoLimit() {
    LoadLimiter limiter = new LoadLimiter(null);
    for (int i = 0; i < 100; i++) {
      assertTrue(limiter.allow());
    }
  }

  /** R139: no subcommand prints the banner, the usage and the help, and exits zero. */
  @Test
  void noSubcommandPrintsTheBannerAndExitsZero() {
    JsonNode recorded = SourceAnswers.get("cli_surface");
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    OpalCli cli =
        new OpalCli(OpalCli.Which.server, Map.of(), new PrintStream(captured, true,
            StandardCharsets.UTF_8));
    assertEquals(0, cli.run(new String[] {}));
    String out = captured.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("OPAL-SERVER"));
    assertTrue(out.contains("Usage: opal-server [OPTIONS] COMMAND [ARGS]..."));
    assertTrue(out.contains("Config top level options:"));
    assertEquals(recorded.get("no_subcommand_exit").asInt(), 0);
    assertTrue(recorded.get("no_subcommand_mentions_usage").asBoolean());
  }

  /** R134: six commands on each of the two, plus every configuration entry as an option. */
  @Test
  void theCommandAndOptionSurfaceMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("cli_surface");
    for (OpalCli.Which which : OpalCli.Which.values()) {
      ByteArrayOutputStream captured = new ByteArrayOutputStream();
      OpalCli cli =
          new OpalCli(which, Map.of(), new PrintStream(captured, true, StandardCharsets.UTF_8));
      cli.run(new String[] {});
      String out = captured.toString(StandardCharsets.UTF_8);
      for (String command :
          List.of("run", "print-config", "obtain-token", "generate-secret",
              "publish-data-update", "version")) {
        assertTrue(out.contains("  " + command + "\n"), which + " offers " + command);
      }
      assertTrue(out.contains("--log-level"), which + " offers the common entries as options");
    }
    for (String key : List.of("opal_server.cli", "opal_client.cli")) {
      assertEquals(
          List.of(
              "generate-secret", "obtain-token", "print-config", "publish-data-update", "run",
              "version"),
          SourceAnswers.strings(recorded.get(key).get("commands")),
          key);
      assertEquals(0, recorded.get(key).get("exit").asInt(), key + " exits zero");
    }
  }

  /** R135: URL-safe by default, hex on request, and a Python bytes repr when asked. */
  @Test
  void generatedSecretsHaveTheSourcesShapes() {
    JsonNode recorded = SourceAnswers.get("cli_surface");
    assertEquals(
        recorded.get("generate_secret_default_len").asInt(),
        run("generate-secret", "--size", "8").trim().length());
    assertEquals(
        recorded.get("generate_secret_hex_len").asInt(),
        run("generate-secret", "--size", "8", "--format", "hex").trim().length());
    assertTrue(run("generate-secret", "--size", "8", "--format", "bytes").trim().startsWith("b'"));
  }

  /** R137: with neither a source URL nor entries, it says so and does nothing. */
  @Test
  void publishingWithNoArgumentsSaysSo() {
    JsonNode recorded = SourceAnswers.get("cli_surface");
    String expected = recorded.get("publish_no_args").asText().split("\\R")[0];
    assertTrue(run("publish-data-update").contains(expected));
  }

  /** R138: the version of the common package. */
  @Test
  void versionMatchesTheSource() {
    JsonNode recorded = SourceAnswers.get("cli_surface");
    assertEquals(recorded.get("version").asText().trim(), run("version").trim());
  }

  private static String run(String... args) {
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    OpalCli cli =
        new OpalCli(
            OpalCli.Which.server, Map.of(), new PrintStream(captured, true, StandardCharsets.UTF_8));
    cli.run(args);
    return captured.toString(StandardCharsets.UTF_8);
  }
}
