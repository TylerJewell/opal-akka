package io.akka.opal.common.confi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.opal.common.util.Repr;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * A set of typed configuration entries read from the environment — SPEC-002 R1 to R12.
 *
 * <p>Declaration order is load order: entries are evaluated in the order they are declared, and
 * an entry whose default is <em>delayed</em> is evaluated after all of them, against the values
 * they ended up with. A delayed default is skipped entirely when the environment supplied a
 * value, so an operator's setting is never overwritten by a template.
 *
 * <p>The prefix is applied to every environment key and never to the name — the name is what
 * {@code print-config} reports.
 */
public class Confi {

  /** Raised when a value is present but is not of the entry's type. */
  public static final class BadValue extends RuntimeException {
    public BadValue(String message) {
      super(message);
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Confi.class);

  private final String prefix;
  private final Map<String, String> environment;
  private final Map<String, ConfiEntry<?>> entries = new LinkedHashMap<>();
  private final List<ConfiEntry<?>> delayed = new ArrayList<>();

  protected Confi(String prefix, Map<String, String> environment) {
    this.prefix = prefix == null ? "" : prefix;
    this.environment = environment;
  }

  /** Keyed by entry name, in declaration order. */
  public Map<String, ConfiEntry<?>> entries() {
    return entries;
  }

  public ObjectMapper mapper() {
    return MAPPER;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String name) {
    ConfiEntry<?> entry = entries.get(name);
    return entry == null ? null : (T) entry.value();
  }

  public String getString(String name) {
    Object v = get(name);
    return v == null ? null : String.valueOf(v);
  }

  /**
   * Fills in every delayed default. Called once, by the owning configuration class, after all of
   * its entries have been declared.
   */
  protected void resolveDelayed() {
    for (ConfiEntry<?> entry : delayed) {
      if (raw(entry.key()) != null) {
        continue;
      }
      @SuppressWarnings("unchecked")
      ConfiEntry<Object> typed = (ConfiEntry<Object>) entry;
      Object produced = entry.delayed().apply(this);
      typed.set(produced instanceof String s ? typed.cast().apply(s) : produced);
    }
  }

  private String raw(String key) {
    return environment.get(prefix + key);
  }

  // -- declaration ---------------------------------------------------------

  protected <T> ConfiEntry<T> add(ConfiEntry<T> entry) {
    entries.put(entry.name(), entry);
    @SuppressWarnings("unchecked")
    ConfiEntry<Object> typed = (ConfiEntry<Object>) entry;
    String raw = raw(entry.key());
    if (entry.isDelayed()) {
      delayed.add(entry);
      if (raw != null) {
        typed.set(cast(entry, raw));
      }
    } else {
      typed.set(raw != null ? cast(entry, raw) : entry.rawDefault());
    }
    return entry;
  }

  /**
   * R270: a value that will not cast names the entry it came from before the failure escapes.
   *
   * <p>The cast function has only the value, so the message it raises says what was wrong and not
   * where it was read; an operator reading a start-up failure needs the key.
   */
  private Object cast(ConfiEntry<?> entry, String raw) {
    try {
      return entry.cast().apply(raw);
    } catch (RuntimeException e) {
      LOG.error("Failed parsing config key- {}", entry.key());
      throw e;
    }
  }

  protected ConfiEntry<String> str(String name, String def, String description) {
    return str(name, name, def, description, null);
  }

  protected ConfiEntry<String> str(
      String name, String key, String def, String description, List<String> flags) {
    return add(new ConfiEntry<>(name, key, description, "str", s -> s, def, null, flags));
  }

  protected ConfiEntry<String> strDelayed(
      String name, Function<Confi, Object> delayedDefault, String description) {
    return add(new ConfiEntry<>(name, name, description, "str", s -> s, null, delayedDefault, null));
  }

  protected ConfiEntry<Integer> integer(String name, Integer def, String description) {
    return add(new ConfiEntry<>(name, name, description, "int", Confi::castInt, def, null, null));
  }

  protected ConfiEntry<Double> decimal(String name, Double def, String description) {
    return add(
        new ConfiEntry<>(name, name, description, "float", Confi::castDouble, def, null, null));
  }

  protected ConfiEntry<Boolean> bool(String name, Boolean def, String description) {
    return bool(name, def, description, null);
  }

  protected ConfiEntry<Boolean> bool(
      String name, Boolean def, String description, List<String> flags) {
    return add(
        new ConfiEntry<>(name, name, description, "bool", Confi::castBoolean, def, null, flags));
  }

  protected ConfiEntry<List<String>> list(String name, List<String> def, String description) {
    return list(name, def, description, ",");
  }

  /**
   * A list entry with a separator of its own.
   *
   * <p>One entry in OPAL is colon-separated rather than comma-separated, because the values are
   * directory paths and a comma is legal in one.
   */
  protected ConfiEntry<List<String>> list(
      String name, List<String> def, String description, String delimiter) {
    return add(
        new ConfiEntry<>(
            name,
            name,
            description,
            "list",
            value -> castList(value, delimiter),
            def,
            null,
            null));
  }

  protected <T> ConfiEntry<T> model(String name, Class<T> type, T def, String description) {
    return add(
        new ConfiEntry<>(
            name,
            name,
            description,
            type.getSimpleName(),
            s -> castModel(s, type),
            def,
            null,
            null));
  }

  protected <T> ConfiEntry<T> modelDelayed(
      String name, Class<T> type, Function<Confi, Object> delayedDefault, String description) {
    return add(
        new ConfiEntry<>(
            name,
            name,
            description,
            type.getSimpleName(),
            s -> castModel(s, type),
            null,
            delayedDefault,
            null));
  }

  protected <E extends Enum<E>> ConfiEntry<E> enumeration(
      String name, Class<E> type, E def, String description) {
    return add(
        new ConfiEntry<>(
            name, name, description, type.getSimpleName(), s -> castEnum(s, type), def, null, null));
  }

  /**
   * A key entry — R8: the value is either a path to read or the key text itself, and text with
   * no newline in it has every underscore turned back into one.
   */
  protected ConfiEntry<String> key(
      String name,
      String description,
      java.util.function.Supplier<Object> format,
      boolean isPublic) {
    return add(
        new ConfiEntry<>(
            name,
            name,
            description,
            "Union",
            s -> {
              try {
                return Keys.decode(s, String.valueOf(format.get()), isPublic);
              } catch (RuntimeException e) {
                LOG.error("could not read " + name, e);
                throw e;
              }
            },
            null,
            null,
            null));
  }

  // -- casts ---------------------------------------------------------------

  /**
   * R2: {@code true} or {@code 1} in any casing, {@code false} or {@code 0} in any casing, and
   * nothing else. Enumerated rather than delegated to a permissive parser, because {@code yes},
   * {@code on} and the empty string are failures in the source and a parser that accepted them
   * would give a deployment a different answer.
   */
  public static boolean castBoolean(String value) {
    switch (value.toLowerCase(java.util.Locale.ROOT)) {
      case "true":
      case "1":
        return true;
      case "false":
      case "0":
        return false;
      default:
        throw new BadValue(value.toLowerCase(java.util.Locale.ROOT) + " - is not a valid boolean");
    }
  }

  public static int castInt(String value) {
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new BadValue(value + " - is not a valid int");
    }
  }

  public static double castDouble(String value) {
    try {
      return Double.parseDouble(value.trim());
    } catch (NumberFormatException e) {
      throw new BadValue(value + " - is not a valid float");
    }
  }

  /** R3: comma separated, each element stripped of surrounding whitespace. */
  public static List<String> castList(String value) {
    return castList(value, ",");
  }

  public static List<String> castList(String value, String delimiter) {
    if (value.isEmpty()) {
      return List.of();
    }
    return Arrays.stream(value.split(java.util.regex.Pattern.quote(delimiter), -1))
        .map(String::strip)
        .toList();
  }

  public static <T> T castModel(String value, Class<T> type) {
    try {
      return MAPPER.readValue(value, type);
    } catch (Exception e) {
      throw new BadValue("Failed parsing config value as " + type.getSimpleName() + ": " + e);
    }
  }

  public static <T> T castModel(String value, TypeReference<T> type) {
    try {
      return MAPPER.readValue(value, type);
    } catch (Exception e) {
      throw new BadValue("Failed parsing config value: " + e);
    }
  }

  private static <E extends Enum<E>> E castEnum(String value, Class<E> type) {
    for (E candidate : type.getEnumConstants()) {
      if (candidate.name().equalsIgnoreCase(value)
          || candidate.toString().equalsIgnoreCase(value)) {
        return candidate;
      }
    }
    throw new BadValue(value + " - is not a valid " + type.getSimpleName());
  }

  // -- reporting -----------------------------------------------------------

  /** R10: the entries as JSON, names sorted, every value stringified. */
  public String printConfig() {
    Map<String, String> sorted = new TreeMap<>();
    for (Map.Entry<String, ConfiEntry<?>> e : entries.entrySet()) {
      sorted.put(e.getKey(), Repr.python(e.getValue().value()));
    }
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(sorted);
    } catch (Exception ex) {
      return sorted.toString();
    }
  }

  /**
   * R271: the entries with each value's {@code repr} rather than its string form.
   *
   * <p>Sorted by name, four spaces of indent, and the class's own name on the first line. More
   * accurate than {@link #printConfig()} for a value whose string form loses its type — an empty
   * string and a null read the same there and not here.
   */
  /**
   * The name this configuration object reports as its own.
   *
   * <p>R271 prints it, so it is part of what a caller diffing the two systems' {@code debug_repr}
   * sees. The source's own class names are what that caller has seen before, so those are what is
   * reported rather than this rebuild's.
   */
  protected String configName() {
    return getClass().getSimpleName();
  }

  public String debugRepr() {
    StringBuilder text = new StringBuilder(configName()).append("(Confi):\n");
    for (String name : new TreeMap<>(entries).keySet()) {
      text.append("    ")
          .append(name)
          .append(": ")
          .append(Repr.repr(entries.get(name).value()))
          .append('\n');
    }
    return text.toString();
  }

  /**
   * R11: a value given on the command line overrides the environment, matched by the entry's
   * key rather than its name. Where the two differ the lookup finds nothing and the value is
   * dropped, which is what the source does with the same three entries.
   */
  public boolean applyCommandLine(String key, String rawValue) {
    ConfiEntry<?> entry = entries.get(key);
    if (entry == null) {
      return false;
    }
    @SuppressWarnings("unchecked")
    ConfiEntry<Object> typed = (ConfiEntry<Object>) entry;
    typed.set(typed.cast().apply(rawValue));
    return true;
  }
}
