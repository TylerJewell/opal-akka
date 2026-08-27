package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.http.javadsl.model.ContentTypes;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * SPEC-002 R193 — the three data routes answer wherever their configuration entries put them.
 *
 * <p>This is an integration test rather than a unit one on purpose. The dispatch it checks is
 * reachable only if the runtime routes a path the annotations do not name to the method that
 * decides, and a unit test of that method would pass whether or not anything ever called it —
 * the exact shape `PIPELINE.md` describes as a rule specified, unit-tested and never reached.
 */
@ExtendWith(OpalProcessExtension.class)
public class ConfiguredRoutesIntegrationTest extends TestKitSupport {

  private static final byte[] EMPTY_REPORT =
      "{\"reports\": []}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

  static void startProcess() {
    System.setProperty("OPAL_ROLE", "server");
    System.setProperty("OPAL_ALL_DATA_ROUTE", "/moved-policy-data");
    System.setProperty("OPAL_DATA_CONFIG_ROUTE", "/elsewhere/data/config");
  }

  @AfterAll
  public static void clearProperties() {
    System.clearProperty("OPAL_ROLE");
    System.clearProperty("OPAL_ALL_DATA_ROUTE");
    System.clearProperty("OPAL_DATA_CONFIG_ROUTE");
  }

  /** R131 and R193: the stand-in data source answers where the entry moved it. */
  @Test
  public void theAllDataRouteAnswersWhereItsEntryPutsIt() {
    StrictResponse<ByteString> moved = httpClient.GET("/moved-policy-data").invoke();
    assertEquals(200, moved.status().intValue());
    assertEquals("{}", moved.body().utf8String());
  }

  /** R56 and R193: two segments deep, and the answer is the route's own, not a 404. */
  @Test
  public void theDataConfigRouteAnswersWhereItsEntryPutsIt() {
    StrictResponse<ByteString> moved = httpClient.GET("/elsewhere/data/config").invoke();
    // Unauthenticated, so the answer is the route's refusal rather than the route's absence —
    // which is the distinction this test exists to draw.
    assertTrue(
        moved.status().intValue() == 200 || moved.status().intValue() == 401,
        "expected the route's own answer, got " + moved.status());
    assertTrue(moved.body().utf8String().contains("detail")
            || moved.body().utf8String().startsWith("{"),
        moved.body().utf8String());
  }

  /** A path nothing configured is still a 404, so the dispatch did not become a catch-all. */
  @Test
  public void anUnconfiguredPathIsStillNotFound() {
    StrictResponse<ByteString> nothing = httpClient.GET("/nothing-is-here").invoke();
    assertEquals(404, nothing.status().intValue());
  }

  /**
   * R193: the compiled default stops answering once its entry has moved it.
   *
   * <p>The source mounts each of these routes at the path its entry names, so moving the entry
   * moves the route. A rebuild whose annotated path kept answering would have the route in two
   * places where the original has it in one.
   */
  @Test
  public void theCompiledDefaultStopsAnsweringOnceItsEntryMoved() {
    StrictResponse<ByteString> original = httpClient.GET("/policy-data").invoke();
    assertEquals(404, original.status().intValue(), "the entry no longer names this path");
    StrictResponse<ByteString> config = httpClient.GET("/data/config").invoke();
    assertEquals(404, config.status().intValue(), "the entry no longer names this path");
  }

  /** A route whose entry nobody moved is unaffected. */
  @Test
  public void anUnmovedRouteStillAnswersItsCompiledPath() {
    StrictResponse<ByteString> callback =
        httpClient
            .POST("/data/callback_report")
            .withRequestBody(ContentTypes.APPLICATION_JSON, EMPTY_REPORT)
            .invoke();
    assertTrue(
        callback.status().intValue() == 200 || callback.status().intValue() == 422,
        "expected the route's own answer, got " + callback.status());
  }
}
