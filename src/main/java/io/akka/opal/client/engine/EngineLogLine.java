package io.akka.opal.client.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.common.config.Enums.EngineLogFormat;
import java.util.Iterator;
import java.util.Map;

/**
 * One line of engine output, rendered the way the configured format renders it — SPEC-002
 * R163–R166.
 *
 * <p>OPA writes JSON, one object per line, with a `level` and usually a `msg`. Each format reads
 * a different part of it: `MINIMAL` prints the event name alone, `HTTP` adds the request it
 * describes, and `FULL` prints everything the line carried besides those. Where the chosen format
 * cannot find what it needs — a line with no `msg`, or an `HTTP` line that is not about a request
 * — the whole object is printed instead, so nothing is lost by choosing a narrower format.
 *
 * <p>The fields a format consumes are removed before the remainder is printed, which is why the
 * fallback from `HTTP` does not repeat the method, path and status.
 */
public final class EngineLogLine {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** What to log, and at what level. A `null` rendering means the line was not JSON. */
  public record Rendered(String level, String text) {}

  private EngineLogLine() {}

  /**
   * R164: the level name a line's own `level` field resolves to.
   *
   * <p>These are the names the source's logging library uses, and both spellings of two of them
   * collapse: `warn` and `warning` are one level, `fatal` and `critical` are another. Anything
   * else, including an absent field, is `INFO`.
   *
   * <p>`CRITICAL` is a level this target does not have; {@link #severity} says what it is logged
   * at. The name is kept because it is what the source computed, and it is what the answers
   * recorded from the source carry.
   */
  public static String level(String name) {
    if (name == null) {
      return "INFO";
    }
    return switch (name.toLowerCase()) {
      case "critical", "fatal" -> "CRITICAL";
      case "error" -> "ERROR";
      case "warning", "warn" -> "WARNING";
      case "debug" -> "DEBUG";
      default -> "INFO";
    };
  }

  /** Where a level name lands on a target whose highest severity is `error`. */
  public static String severity(String levelName) {
    return "CRITICAL".equals(levelName) ? "ERROR" : levelName;
  }

  /** R163: the text a format produces for a line, or the raw line when it is not JSON. */
  public static Rendered render(String line, EngineLogFormat format) {
    if (format == null || format == EngineLogFormat.NONE) {
      return null;
    }
    ObjectNode fields;
    try {
      JsonNode parsed = MAPPER.readTree(line);
      if (parsed == null || !parsed.isObject()) {
        return new Rendered("INFO", line);
      }
      fields = (ObjectNode) parsed;
    } catch (Exception e) {
      return new Rendered("INFO", line);
    }

    String level = level(text(fields.remove("level")));
    String msg = text(fields.remove("msg"));

    String rendered = null;
    if (format == EngineLogFormat.MINIMAL) {
      rendered = eventName(msg);
    } else if (format == EngineLogFormat.HTTP) {
      rendered = httpDetails(msg, fields);
    }
    if (rendered == null) {
      rendered = entireObject(msg, fields);
    }
    return new Rendered(level, rendered);
  }

  /** R165: the event name alone, padded, or nothing to say when there is no name. */
  static String eventName(String msg) {
    return msg == null ? null : pad(msg);
  }

  /**
   * R166: the request a line describes.
   *
   * <p>The three request fields are taken out of the object whether or not they are all present,
   * so a line this format cannot render falls back without repeating them.
   */
  static String httpDetails(String msg, ObjectNode fields) {
    String method = text(fields.remove("req_method"));
    String path = text(fields.remove("req_path"));
    JsonNode status = fields.remove("resp_status");
    if (msg == null || method == null || path == null) {
      return null;
    }
    if (status == null || status.isNull()) {
      return pad(msg) + " " + method + " " + path;
    }
    return pad(msg) + " " + method + " " + path + " -> " + status.asText();
  }

  /** Everything the line carried that a format did not consume. */
  static String entireObject(String msg, ObjectNode fields) {
    String body = writeObject(fields);
    return msg == null ? body : pad(msg) + " " + body;
  }

  /**
   * The remaining fields as JSON, in the order the line had them.
   *
   * <p>Written by hand rather than through the mapper because the separators are part of the
   * output: the source's serialiser writes {@code ", "} between entries and {@code ": "} after a
   * key, and a mapper writes neither by default.
   */
  private static String writeObject(JsonNode fields) {
    StringBuilder out = new StringBuilder("{");
    Iterator<Map.Entry<String, JsonNode>> entries = fields.fields();
    boolean first = true;
    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      if (!first) {
        out.append(", ");
      }
      out.append('"').append(entry.getKey()).append("\": ").append(writeValue(entry.getValue()));
      first = false;
    }
    return out.append('}').toString();
  }

  /** The same separators all the way down: a nested object is written like the outer one. */
  private static String writeValue(JsonNode value) {
    if (value.isObject()) {
      return writeObject(value);
    }
    if (value.isArray()) {
      StringBuilder out = new StringBuilder("[");
      for (int index = 0; index < value.size(); index++) {
        if (index > 0) {
          out.append(", ");
        }
        out.append(writeValue(value.get(index)));
      }
      return out.append(']').toString();
    }
    return value.toString();
  }

  private static String pad(String value) {
    return value.length() >= 20 ? value : value + " ".repeat(20 - value.length());
  }

  private static String text(JsonNode node) {
    return node == null || node.isNull() ? null : node.asText();
  }
}
