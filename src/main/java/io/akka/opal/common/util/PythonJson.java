package io.akka.opal.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;

/**
 * JSON in the exact byte form OPAL hashes: an item separator of {@code ", "}, a key separator
 * of {@code ": "}, and every non-ASCII character escaped.
 *
 * <p>The separators are load-bearing rather than cosmetic. A data-entry report carries a
 * SHA-256 of the fetched value, and a fleet running both systems compares those hashes to
 * decide whether two clients hold the same document; a writer that omitted the spaces would
 * produce a different digest for every value that is not a bare string.
 */
public final class PythonJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PythonJson() {}

  public static String dumps(Object value) {
    StringBuilder out = new StringBuilder();
    write(out, MAPPER.valueToTree(value));
    return out.toString();
  }

  public static String dumps(JsonNode node) {
    StringBuilder out = new StringBuilder();
    write(out, node);
    return out.toString();
  }

  private static void write(StringBuilder out, JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      out.append("null");
    } else if (node.isObject()) {
      out.append('{');
      boolean first = true;
      for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> field = it.next();
        if (!first) {
          out.append(", ");
        }
        first = false;
        writeString(out, field.getKey());
        out.append(": ");
        write(out, field.getValue());
      }
      out.append('}');
    } else if (node.isArray()) {
      out.append('[');
      boolean first = true;
      for (JsonNode item : node) {
        if (!first) {
          out.append(", ");
        }
        first = false;
        write(out, item);
      }
      out.append(']');
    } else if (node.isTextual()) {
      writeString(out, node.textValue());
    } else if (node.isBoolean()) {
      out.append(node.booleanValue() ? "true" : "false");
    } else if (node.isIntegralNumber()) {
      out.append(node.bigIntegerValue().toString());
    } else {
      out.append(formatDouble(node.doubleValue()));
    }
  }

  /** Python's float repr: an integral value keeps its {@code .0}, everything else is shortest. */
  static String formatDouble(double value) {
    if (value == Math.rint(value) && !Double.isInfinite(value) && Math.abs(value) < 1e16) {
      return (long) value + ".0";
    }
    String text = Double.toString(value);
    if (text.contains("E")) {
      String[] parts = text.split("E");
      String exponent = parts[1];
      String sign = exponent.startsWith("-") ? "-" : "+";
      String digits = exponent.startsWith("-") ? exponent.substring(1) : exponent;
      if (digits.length() < 2) {
        digits = "0" + digits;
      }
      text = parts[0] + "e" + sign + digits;
    }
    return text;
  }

  private static void writeString(StringBuilder out, String text) {
    out.append('"');
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        default -> {
          if (c < 0x20 || c > 0x7e) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    out.append('"');
  }
}
