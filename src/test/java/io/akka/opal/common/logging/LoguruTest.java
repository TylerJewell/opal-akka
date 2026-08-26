package io.akka.opal.common.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.WireAnswers;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * R169–R178: what the eighteen logging entries decide.
 *
 * <p>The expected lines were recorded by calling the source's own {@code configure_logs()} with
 * the environment set each way and a capturing sink attached afterwards
 * ({@code probes/complete/log_render.py}), so what is compared is the text a deployment would
 * see rather than a reading of the format string.
 */
class LoguruTest {

  private static Loguru.Record record(String name, String level, String message) {
    return new Loguru.Record(
        ZonedDateTime.of(2026, 8, 26, 12, 48, 59, 928114000, ZoneOffset.ofHours(-7)),
        10041,
        "MainThread",
        1,
        name,
        level,
        message,
        null,
        "render",
        "log_render.py",
        45,
        "log_render",
        3.25);
  }

  /** The first line of each recorded run, which is the same record every time. */
  private static String firstRecorded(String key) {
    String all = WireAnswers.get("log_render").get(key).asText();
    int newline = all.indexOf('\n');
    return newline < 0 ? all : all.substring(0, newline);
  }

  /**
   * The instant is the one field that cannot agree — the recording was made at a different one —
   * so it is compared for shape and the rest of the line for text.
   */
  private static final String INSTANT =
      "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}[+-]\\d{4}";

  @Test
  void theDefaultFormatRendersTheSourcesLine() {
    String expected = firstRecorded("no_colorize");
    String actual =
        Loguru.render(Loguru.FORMAT_WITHOUT_PID, record("opal_server.server", "INFO", "hello"),
            false)
            .strip();
    assertTrue(expected.matches(INSTANT + " \\| .*"), expected);
    assertTrue(actual.matches(INSTANT + " \\| .*"), actual);
    assertEquals(expected.replaceFirst(INSTANT, "T"), actual.replaceFirst(INSTANT, "T"));
  }

  @Test
  void includingTheProcessIdPutsItAfterTheTime() {
    String withPid =
        Loguru.render(Loguru.FORMAT_WITH_PID, record("opal_server.server", "INFO", "hello"), false);
    assertTrue(withPid.contains(" | 10041 | "), withPid);
  }

  @Test
  void colourisingWrapsTheSameText() {
    String coloured =
        Loguru.render(Loguru.FORMAT_WITHOUT_PID, record("opal_server.server", "INFO", "hello"),
            true);
    String plain =
        Loguru.render(Loguru.FORMAT_WITHOUT_PID, record("opal_server.server", "INFO", "hello"),
            false);
    // Colour is an escape character and an instruction after it; taking both off must leave
    // exactly the uncoloured line, or the colouring has moved something.
    assertEquals(
        plain, coloured.replaceAll(Character.toString(27) + "\\[[0-9;]*m", ""));
  }

  /** R171: a name too long loses its middle, and one with no dots is cut instead. */
  @Test
  void aLongNameIsShortenedTheWayTheSourceShortensIt() {
    assertEquals("a...forty", Loguru.shortenName("a.very.long.module.name.that.will.not.fit.in.forty"));
    assertEquals(
        "averylongsinglemodulenamewithnodotsat...",
        Loguru.shortenName("averylongsinglemodulenamewithnodotsatallwhichmustbecut"));
    assertEquals("opal_server.server", Loguru.shortenName("opal_server.server"));
  }

  /** R172: the three alignments, and a value already wider than the field. */
  @Test
  void alignmentMatchesTheFormatSpec() {
    assertEquals("ab        ", Loguru.align("ab", "<10"));
    assertEquals("        ab", Loguru.align("ab", ">10"));
    assertEquals("    ab    ", Loguru.align("ab", "^10"));
    assertEquals(" INFO ", Loguru.align("INFO", "^6"));
    assertEquals("WARNING", Loguru.align("WARNING", "^6"));
    assertEquals("ab", Loguru.align("ab", ""));
  }

  /** R174: the include list is read first, so one part of an excluded library still logs. */
  @Test
  void theIncludeListWinsOverTheExcludeList() {
    List<String> exclude = List.of("uvicorn");
    List<String> include = List.of("uvicorn.access");
    assertTrue(Loguru.allowed("opal_server.server", include, exclude));
    assertTrue(Loguru.allowed("uvicorn.access", include, exclude));
    assertFalse(Loguru.allowed("uvicorn.error", include, exclude));
    assertFalse(Loguru.allowed("uvicorn", include, exclude));
  }

  /** R174: what the recorded runs show the two lists doing, line for line. */
  @Test
  void theRecordedRunsShowTheSameFiltering() {
    String excluded = WireAnswers.get("log_render").get("exclude_uvicorn").asText();
    assertFalse(excluded.contains("an access line"));
    assertFalse(excluded.contains("an error line"));
    String both = WireAnswers.get("log_render").get("exclude_include").asText();
    assertTrue(both.contains("an access line"));
    assertFalse(both.contains("an error line"));
  }

  /** R177: the default level hides debug, and naming it shows it. */
  @Test
  void theLevelDecidesWhatIsWritten() {
    assertFalse(WireAnswers.get("log_render").get("defaults").asText().contains("a debug line"));
    assertTrue(WireAnswers.get("log_render").get("level_debug").asText().contains("a debug line"));
    assertEquals(ch.qos.logback.classic.Level.DEBUG, Logs.level("DEBUG"));
    assertEquals(ch.qos.logback.classic.Level.WARN, Logs.level("WARNING"));
    assertEquals(ch.qos.logback.classic.Level.ERROR, Logs.level("CRITICAL"));
    assertEquals(ch.qos.logback.classic.Level.INFO, Logs.level("nonsense"));
  }

  /** R175: a credential inside a URL never reaches a sink, whoever wrote the line. */
  @Test
  void credentialsAreScrubbedFromEveryLine() {
    assertEquals(
        "https://***@example.com/x?token=abc",
        Loguru.scrub("https://user:pw@example.com/x?token=abc"));
    assertTrue(
        WireAnswers.get("log_render").get("defaults").asText()
            .contains("https://***@example.com/x?token=abc"));
  }

  /** R176: the serialised form carries the rendered text and the record beside it. */
  @Test
  void theSerialisedFormHasTheSourcesShape() {
    var serialised = Loguru.serialized(record("opal_server.server", "INFO", "hello"), "text\n");
    assertEquals("text\n", serialised.get("text"));
    @SuppressWarnings("unchecked")
    var fields = (java.util.Map<String, Object>) serialised.get("record");
    assertEquals(
        List.of("elapsed", "exception", "extra", "file", "function", "level", "line", "message",
            "module", "name", "process", "thread", "time"),
        new java.util.ArrayList<>(fields.keySet()));
    assertEquals("hello", fields.get("message"));
  }

  /** R178: the rotation size and the retention window, as the source spells them. */
  @Test
  void theFileSinkReadsTheSourcesSpellings() {
    assertEquals("250MB", Logs.normaliseSize("250 MB"));
    assertEquals(10, Logs.days("10 days"));
    assertEquals(14, Logs.days("2 weeks"));
    assertEquals(60, Logs.days("2 months"));
    assertEquals(10, Logs.days("whenever"));
  }
}
