package io.akka.opal.common.logging;

import io.akka.opal.common.util.Urls;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The pieces of a log line the source's format string names — SPEC-002 R170–R174.
 *
 * <p>`LOG_FORMAT` is a template with `{field}` placeholders, each optionally carrying an
 * alignment and a width, and colour tags between them. Rendering it is what makes eighteen
 * `LOG_*` entries mean anything, so it is done here, away from any logging framework, where each
 * rule can be put to a value directly.
 */
public final class Loguru {

  /** The two formats the source ships, which are what a deployment gets unless it says otherwise. */
  public static final String FORMAT_WITHOUT_PID =
      "<green>{time}</green> | <blue>{name: <40}</blue>|<level>{level:^6} | {message}</level>\n{exception}";

  public static final String FORMAT_WITH_PID =
      "<green>{time}</green> | {process} | <blue>{name: <40}</blue>|<level>{level:^6} | {message}</level>\n{exception}";

  /** R171: a name longer than this is shortened before it is padded. */
  static final int MAX_NAME_LENGTH = 40;

  private static final DateTimeFormatter TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ");

  /** The escape a terminal reads a colour instruction after. */
  private static final String ESC = String.valueOf((char) 27);

  private static final Map<String, String> ANSI =
      Map.of(
          "green", ESC + "[32m",
          "blue", ESC + "[34m",
          "red", ESC + "[31m",
          "yellow", ESC + "[33m",
          "cyan", ESC + "[36m",
          "bold", ESC + "[1m");

  /** The colour a level is written in, which the `<level>` tag stands for. */
  private static final Map<String, String> LEVEL_COLOUR =
      Map.of(
          "DEBUG", ESC + "[34m" + ESC + "[1m",
          "INFO", ESC + "[1m",
          "WARNING", ESC + "[33m" + ESC + "[1m",
          "ERROR", ESC + "[31m" + ESC + "[1m",
          "CRITICAL", ESC + "[31m" + ESC + "[1m");

  private static final String RESET = ESC + "[0m";

  private Loguru() {}

  /** Everything one record offers the format, by the name the format calls it. */
  public record Record(
      ZonedDateTime time,
      long processId,
      String threadName,
      long threadId,
      String name,
      String level,
      String message,
      String exception,
      String function,
      String file,
      int line,
      String module,
      double elapsedSeconds) {}

  /**
   * R170: the record rendered through the format.
   *
   * <p>A placeholder the format names and this does not know is left as it was written, which is
   * what keeps an unrecognised format visible rather than silently empty.
   */
  public static String render(String format, Record record, boolean colorize) {
    StringBuilder out = new StringBuilder();
    int at = 0;
    List<String> openTags = new ArrayList<>();
    while (at < format.length()) {
      char c = format.charAt(at);
      if (c == '{') {
        int end = format.indexOf('}', at);
        if (end < 0) {
          out.append(format.substring(at));
          break;
        }
        out.append(field(format.substring(at + 1, end), record));
        at = end + 1;
      } else if (c == '<') {
        int end = format.indexOf('>', at);
        if (end < 0) {
          out.append(format.substring(at));
          break;
        }
        String tag = format.substring(at + 1, end);
        if (colorize) {
          out.append(colour(tag, record.level(), openTags));
        }
        at = end + 1;
      } else {
        out.append(c);
        at++;
      }
    }
    return out.toString();
  }

  private static String colour(String tag, String level, List<String> openTags) {
    if (tag.startsWith("/")) {
      if (!openTags.isEmpty()) {
        openTags.remove(openTags.size() - 1);
      }
      return RESET;
    }
    openTags.add(tag);
    if (tag.equals("level")) {
      return LEVEL_COLOUR.getOrDefault(level, "");
    }
    String plain = tag.startsWith("fg ") ? "" : ANSI.getOrDefault(tag, "");
    return plain;
  }

  /** One `{name: <40}`-shaped placeholder: the field, then the alignment it asked for. */
  static String field(String token, Record record) {
    String name = token;
    String spec = "";
    int colon = token.indexOf(':');
    if (colon >= 0) {
      name = token.substring(0, colon);
      spec = token.substring(colon + 1);
    }
    String value = value(name, record);
    if (value == null) {
      return "{" + token + "}";
    }
    return align(value, spec);
  }

  private static String value(String name, Record record) {
    return switch (name) {
      case "time" -> record.time().format(TIME);
      case "process" -> String.valueOf(record.processId());
      case "thread" -> record.threadName();
      case "name" -> shortenName(record.name());
      case "level" -> record.level();
      case "message" -> record.message();
      case "exception" -> record.exception() == null ? "" : record.exception();
      case "function" -> record.function();
      case "file" -> record.file();
      case "line" -> String.valueOf(record.line());
      case "module" -> record.module();
      case "elapsed" -> String.valueOf(record.elapsedSeconds());
      default -> null;
    };
  }

  /**
   * R172: `<n` pads on the right, `>n` on the left, `^n` puts the value in the middle.
   *
   * <p>A value already at or past the width is left alone, which is why a seven-character level
   * name overruns a six-column field rather than being cut.
   */
  static String align(String value, String spec) {
    if (spec.isEmpty()) {
      return value;
    }
    String trimmed = spec.trim();
    char alignment = '<';
    if (!trimmed.isEmpty() && (trimmed.charAt(0) == '<' || trimmed.charAt(0) == '>'
        || trimmed.charAt(0) == '^')) {
      alignment = trimmed.charAt(0);
      trimmed = trimmed.substring(1);
    }
    int width;
    try {
      width = Integer.parseInt(trimmed.trim());
    } catch (NumberFormatException e) {
      return value;
    }
    int padding = width - value.length();
    if (padding <= 0) {
      return value;
    }
    return switch (alignment) {
      case '>' -> " ".repeat(padding) + value;
      case '^' -> " ".repeat(padding / 2) + value + " ".repeat(padding - padding / 2);
      default -> value + " ".repeat(padding);
    };
  }

  /**
   * R171: a logger name too long to fit is shortened, first by dropping its middle.
   *
   * <p>`a.very.long.name.in.forty` becomes `a...forty`, which keeps the two ends a reader uses to
   * recognise it. A name with no dots to drop is cut instead, and marked as cut.
   */
  public static String shortenName(String name) {
    String content = name == null ? "" : name;
    if (content.length() > MAX_NAME_LENGTH) {
      String[] parts = content.split("\\.");
      if (parts.length > 2) {
        content = parts[0] + "..." + parts[parts.length - 1];
      }
    }
    if (content.length() > MAX_NAME_LENGTH) {
      content = content.substring(0, MAX_NAME_LENGTH - 3) + "...";
    }
    return content;
  }

  /** R173: the level names the source uses, from the ones this target has. */
  public static String levelName(String slf4jLevel) {
    return "WARN".equals(slf4jLevel) ? "WARNING" : slf4jLevel;
  }

  /**
   * R174: whether a record survives the two module lists.
   *
   * <p>The include list is read first and wins, so a deployment can exclude a whole library and
   * keep one part of it — which is exactly what the shipped defaults do with the web server.
   */
  public static boolean allowed(String name, List<String> includeList, List<String> excludeList) {
    String logger = name == null ? "" : name;
    if (includeList != null) {
      for (String module : includeList) {
        if (logger.startsWith(module)) {
          return true;
        }
      }
    }
    if (excludeList != null) {
      for (String module : excludeList) {
        if (logger.startsWith(module)) {
          return false;
        }
      }
    }
    return true;
  }

  /** R175: credentials embedded in a URL never reach a sink, whoever wrote the text. */
  public static String scrub(String text) {
    return text == null ? null : Urls.redactUrlInText(text, null);
  }

  /** R176: one record as the object `LOG_SERIALIZE` writes instead of a line. */
  public static Map<String, Object> serialized(Record record, String text) {
    Map<String, Object> level = new LinkedHashMap<>();
    level.put("icon", icon(record.level()));
    level.put("name", record.level());
    level.put("no", levelNumber(record.level()));

    Map<String, Object> file = new LinkedHashMap<>();
    file.put("name", record.file());
    file.put("path", record.file());

    Map<String, Object> process = new LinkedHashMap<>();
    process.put("id", record.processId());
    process.put("name", "MainProcess");

    Map<String, Object> thread = new LinkedHashMap<>();
    thread.put("id", record.threadId());
    thread.put("name", record.threadName());

    Map<String, Object> time = new LinkedHashMap<>();
    time.put("repr", record.time().toString());
    time.put("timestamp", record.time().toInstant().toEpochMilli() / 1000.0);

    Map<String, Object> elapsed = new LinkedHashMap<>();
    elapsed.put("seconds", record.elapsedSeconds());

    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("elapsed", elapsed);
    fields.put("exception", record.exception());
    fields.put("extra", Map.of());
    fields.put("file", file);
    fields.put("function", record.function());
    fields.put("level", level);
    fields.put("line", record.line());
    fields.put("message", record.message());
    fields.put("module", record.module());
    fields.put("name", shortenName(record.name()));
    fields.put("process", process);
    fields.put("thread", thread);
    fields.put("time", time);

    Map<String, Object> out = new LinkedHashMap<>();
    out.put("text", text);
    out.put("record", fields);
    return out;
  }

  private static String icon(String level) {
    return switch (level) {
      case "DEBUG" -> "🐞";
      case "WARNING" -> "⚠️";
      case "ERROR" -> "❌";
      case "CRITICAL" -> "☠️";
      default -> "ℹ️";
    };
  }

  private static int levelNumber(String level) {
    return switch (level) {
      case "DEBUG" -> 10;
      case "WARNING" -> 30;
      case "ERROR" -> 40;
      case "CRITICAL" -> 50;
      case "TRACE" -> 5;
      default -> 20;
    };
  }
}
