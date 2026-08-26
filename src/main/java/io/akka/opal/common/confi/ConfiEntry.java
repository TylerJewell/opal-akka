package io.akka.opal.common.confi;

import java.util.List;
import java.util.function.Function;

/**
 * One configuration entry.
 *
 * <p>A name and an environment key are two different things. The name is what the entry is
 * called in the configuration object, and it is what {@code print-config} prints and what a
 * command-line value is written back through; the key is the environment variable's suffix.
 * They match for all but three of OPAL's entries, and where they do not, both behaviours
 * follow the key rather than the name.
 *
 * <p>The value is mutable because the command line writes it after the environment has been
 * read, and because a delayed default is filled in only once every plain entry has a value.
 */
public final class ConfiEntry<T> {

  private final String name;
  private final String key;
  private final String description;
  private final Function<String, T> cast;
  private final Object rawDefault;
  private final List<String> flags;
  private final String typeName;
  private final Function<Confi, Object> delayed;
  private T value;

  ConfiEntry(
      String name,
      String key,
      String description,
      String typeName,
      Function<String, T> cast,
      Object rawDefault,
      Function<Confi, Object> delayed,
      List<String> flags) {
    this.name = name;
    this.key = key;
    this.description = description;
    this.typeName = typeName;
    this.cast = cast;
    this.rawDefault = rawDefault;
    this.delayed = delayed;
    this.flags = flags == null ? List.of() : List.copyOf(flags);
  }

  public String name() {
    return name;
  }

  public String key() {
    return key;
  }

  public String description() {
    return description;
  }

  public String typeName() {
    return typeName;
  }

  public List<String> flags() {
    return flags;
  }

  public Object rawDefault() {
    return rawDefault;
  }

  /** True while this entry's default still has to be evaluated against the other entries. */
  public boolean isDelayed() {
    return delayed != null;
  }

  Function<Confi, Object> delayed() {
    return delayed;
  }

  Function<String, T> cast() {
    return cast;
  }

  public T value() {
    return value;
  }

  public void set(T v) {
    this.value = v;
  }

  /** The option the command line offers: the key lowercased, underscores turned to hyphens. */
  public String optionName() {
    return "--" + key.toLowerCase().replace('_', '-');
  }
}
