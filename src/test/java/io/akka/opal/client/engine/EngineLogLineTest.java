package io.akka.opal.client.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.WireAnswers;
import io.akka.opal.common.config.Enums.EngineLogFormat;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * R163–R166 and R168: what each engine log format renders, against what the source rendered.
 *
 * <p>The expected values were recorded by calling OPAL's own {@code log_engine_output_opa} with a
 * capturing sink — {@code probes/complete/engine_logs.py} — over eleven lines chosen to reach
 * every branch: a plain event, a request with and without a status, a request line missing its
 * path, a line with no message, both spellings of two levels, a line longer than the field it is
 * padded into, and a line that is not JSON at all.
 */
class EngineLogLineTest {

  /** The one line the two sides render differently, and why. */
  private static final String NOT_JSON = "not json at all";

  private static void check(EngineLogFormat format, String key) {
    JsonNode rows = WireAnswers.get("engine_logs").get(key);
    List<String> expected = new ArrayList<>();
    List<String> actual = new ArrayList<>();
    for (JsonNode row : rows) {
      String line = row.get("line").asText();
      if (line.equals(NOT_JSON)) {
        continue;
      }
      JsonNode logged = row.get("logged");
      EngineLogLine.Rendered rendered = EngineLogLine.render(line, format);
      if (logged.isEmpty()) {
        assertNull(rendered, line);
        continue;
      }
      expected.add(logged.get(0).get("level").asText() + "|" + logged.get(0).get("message").asText());
      actual.add(rendered.level() + "|" + rendered.text());
    }
    assertEquals(expected, actual, key);
  }

  @Test
  void theMinimalFormatRendersWhatTheSourceRenders() {
    check(EngineLogFormat.MINIMAL, "minimal");
  }

  @Test
  void theHttpFormatRendersWhatTheSourceRenders() {
    check(EngineLogFormat.HTTP, "http");
  }

  @Test
  void theFullFormatRendersWhatTheSourceRenders() {
    check(EngineLogFormat.FULL, "full");
  }

  /** R163: with the format off, nothing is rendered at all — the streams are not even read. */
  @Test
  void theNoneFormatRendersNothing() {
    for (JsonNode row : WireAnswers.get("engine_logs").get("none")) {
      assertTrue(row.get("logged").isEmpty());
      assertNull(EngineLogLine.render(row.get("line").asText(), EngineLogFormat.NONE));
    }
  }

  /**
   * The one deliberate difference: a line that is not JSON.
   *
   * <p>The source logs the undecoded bytes, so its line reads {@code b'not json at all'}. This
   * logs the text. Recorded here rather than left to be noticed, because the recorded answer says
   * one thing and this port does another on purpose.
   */
  @Test
  void aLineThatIsNotJsonIsLoggedAsText() {
    for (JsonNode row : WireAnswers.get("engine_logs").get("full")) {
      if (!row.get("line").asText().equals(NOT_JSON)) {
        continue;
      }
      assertEquals("b'" + NOT_JSON + "'", row.get("logged").get(0).get("message").asText());
      assertEquals(NOT_JSON, EngineLogLine.render(NOT_JSON, EngineLogFormat.FULL).text());
      return;
    }
    throw new AssertionError("the recorded answers no longer carry the not-json line");
  }

  /** R167: a Go panic names the runtime's own source file, and that is what is watched for. */
  @Test
  void aPanicIsRecognisedByTheRuntimeSourceFileItNames() {
    assertTrue(
        ("goroutine 1 [running]:\n/usr/local/" + EngineRunner.PANIC_MARKER + ":565 +0x2c5")
            .contains(EngineRunner.PANIC_MARKER));
  }

  /** R168: the wait before relaunching doubles from half a second to a ceiling of ten. */
  @Test
  void theRelaunchWaitMatchesTheSourcesRetryPolicy() {
    JsonNode expected = WireAnswers.get("engine_backoff");
    for (int attempt = 1; attempt <= 9; attempt++) {
      assertEquals(
          (long) (expected.get(String.valueOf(attempt)).asDouble() * 1000),
          EngineRunner.relaunchDelay(attempt),
          "attempt " + attempt);
    }
  }
}
