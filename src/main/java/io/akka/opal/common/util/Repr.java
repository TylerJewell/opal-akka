package io.akka.opal.common.util;

import java.util.List;
import java.util.Map;

/**
 * Python's {@code str()} and {@code repr()} for the values a configuration entry can hold.
 *
 * <p>{@code print-config} is a surface: it emits a JSON object whose every value is the
 * stringified entry, and a caller reading it — a deployment's own start-up check, a support
 * request, the configuration-drift test the source ships — reads those strings. So the
 * stringification is reproduced rather than approximated, down to {@code None} for absence and
 * the quoting Python puts around a string inside a container.
 */
public final class Repr {

  /**
   * A value that knows how Python would print it: a model, printed field by field.
   *
   * <p>The two forms are derived from one list of {@code field=value} pairs rather than from
   * each other. Deriving the parenthesised form by re-punctuating the flat one re-punctuates a
   * nested model's commas as well, which is wrong once any field holds another model.
   */
  public interface Reprable {
    /** One {@code field=value} entry per field, in the source's declaration order. */
    List<String> pyFields();

    /** The name Python prints in the parenthesised form. */
    default String pyClassName() {
      return getClass().getSimpleName();
    }

    /** The {@code str()} form: the pairs joined by spaces. */
    default String pyStr() {
      return String.join(" ", pyFields());
    }

    /** The {@code repr()} form: the class name and the same pairs in parentheses. */
    default String pyRepr() {
      return pyClassName() + "(" + String.join(", ", pyFields()) + ")";
    }
  }

  private Repr() {}

  public static String python(Object value) {
    if (value == null) {
      return "None";
    }
    if (value instanceof Reprable r) {
      return r.pyStr();
    }
    if (value instanceof String s) {
      return s;
    }
    if (value instanceof Enum<?> e) {
      return enumStr(e);
    }
    return repr(value);
  }

  public static String repr(Object value) {
    if (value == null) {
      return "None";
    }
    if (value instanceof Reprable r) {
      return r.pyRepr();
    }
    if (value instanceof String s) {
      return quote(s);
    }
    if (value instanceof Boolean b) {
      return b ? "True" : "False";
    }
    if (value instanceof Double d) {
      return PythonJson.formatDouble(d);
    }
    if (value instanceof Float f) {
      return PythonJson.formatDouble(f);
    }
    if (value instanceof Number n) {
      return n.toString();
    }
    if (value instanceof Enum<?> e) {
      return "<" + e.getClass().getSimpleName() + "." + e.name() + ": " + quote(wire(e)) + ">";
    }
    if (value instanceof List<?> list) {
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < list.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(repr(list.get(i)));
      }
      return sb.append(']').toString();
    }
    if (value instanceof Map<?, ?> map) {
      StringBuilder sb = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<?, ?> e : map.entrySet()) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        sb.append(repr(e.getKey())).append(": ").append(repr(e.getValue()));
      }
      return sb.append('}').toString();
    }
    return String.valueOf(value);
  }

  /**
   * Python's set literal, which reaches a caller inside one refusal message: the topics a token
   * was not permitted to publish on are reported as {@code {'b'}} rather than as a list.
   */
  public static String pySet(List<String> items) {
    if (items.isEmpty()) {
      return "set()";
    }
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(quote(items.get(i)));
    }
    return sb.append('}').toString();
  }

  /** An enumeration prints as {@code Class.MEMBER} on its own, and wrapped inside a container. */
  public static String enumStr(Enum<?> value) {
    return value.getClass().getSimpleName() + "." + value.name();
  }

  private static String wire(Enum<?> value) {
    try {
      return String.valueOf(value.getClass().getMethod("wire").invoke(value));
    } catch (ReflectiveOperationException e) {
      return value.name();
    }
  }

  /** Python's string repr: single quotes unless the text holds one and no double quote. */
  public static String quote(String text) {
    boolean single = text.indexOf('\'') >= 0 && text.indexOf('"') < 0;
    char q = single ? '"' : '\'';
    StringBuilder sb = new StringBuilder().append(q);
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == q || c == '\\') {
        sb.append('\\').append(c);
      } else if (c == '\n') {
        sb.append("\\n");
      } else if (c == '\r') {
        sb.append("\\r");
      } else if (c == '\t') {
        sb.append("\\t");
      } else {
        sb.append(c);
      }
    }
    return sb.append(q).toString();
  }
}
