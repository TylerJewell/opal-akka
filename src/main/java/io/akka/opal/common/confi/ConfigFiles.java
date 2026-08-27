package io.akka.opal.common.confi;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The {@code settings.ini} or {@code .env} file a deployment configures OPAL with — SPEC-002 R268.
 *
 * <p>OPAL reads every entry through python-decouple, which looks for {@code settings.ini} and then
 * {@code .env} in one directory, walks to the parent when it finds neither, and stops at the
 * filesystem root. The first file found is the only one read: a {@code .env} beside a
 * {@code settings.ini} is never consulted. An environment variable outranks whatever the file
 * says, and the file outranks the entry's default.
 *
 * <p>The walk starts at the working directory here and at the directory of the module that asked
 * for the first entry in the source — OD-13. A class inside a jar has no such directory, and the
 * working directory is what an operator writing a {@code .env} beside the process means.
 */
public final class ConfigFiles {

  private ConfigFiles() {}

  /** The names one directory is searched for, in the order decouple searches them. */
  private static final List<String> SUPPORTED = List.of("settings.ini", ".env");

  /** The section a {@code settings.ini} keeps its entries under. */
  private static final String SECTION = "settings";

  /**
   * The settings of the first supported file at or above {@code from}, keyed the way a lookup
   * will spell them, or an empty map when there is none.
   */
  public static Map<String, String> read(Path from) {
    Path found = find(from);
    if (found == null) {
      return Map.of();
    }
    try {
      List<String> lines = Files.readAllLines(found, StandardCharsets.UTF_8);
      return found.getFileName().toString().equals(".env") ? env(lines) : upperCased(ini(lines));
    } catch (IOException e) {
      return Map.of();
    }
  }

  private static Map<String, String> upperCased(Map<String, String> settings) {
    Map<String, String> keyed = new LinkedHashMap<>();
    settings.forEach((name, value) -> keyed.put(name.toUpperCase(Locale.ROOT), value));
    return keyed;
  }

  static Path find(Path from) {
    for (Path directory = from; directory != null; directory = directory.getParent()) {
      for (String name : SUPPORTED) {
        Path candidate = directory.resolve(name);
        if (Files.isRegularFile(candidate)) {
          return candidate;
        }
      }
    }
    return null;
  }

  /**
   * A line is a setting when it is not blank, does not start with {@code #}, and holds an
   * {@code =}. The name and the value are both trimmed, and one matching pair of quotes around the
   * value is removed.
   */
  static Map<String, String> env(List<String> lines) {
    Map<String, String> read = new LinkedHashMap<>();
    for (String raw : lines) {
      String line = raw.strip();
      int split = line.indexOf('=');
      if (line.isEmpty() || line.startsWith("#") || split < 0) {
        continue;
      }
      String name = line.substring(0, split).strip();
      String value = line.substring(split + 1).strip();
      if (value.length() >= 2
          && ((value.charAt(0) == '\'' && value.endsWith("'"))
              || (value.charAt(0) == '"' && value.endsWith("\"")))) {
        value = value.substring(1, value.length() - 1);
      }
      read.put(name, value);
    }
    return read;
  }

  /**
   * The {@code [settings]} section of an ini file. Names are matched without regard to case,
   * which is what the parser underneath the source does, so the map is keyed in lower case and
   * {@link #overlay} looks entries up that way.
   */
  static Map<String, String> ini(List<String> lines) {
    Map<String, String> read = new LinkedHashMap<>();
    boolean inSection = false;
    for (String raw : lines) {
      String line = raw.strip();
      if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
        continue;
      }
      if (line.startsWith("[") && line.endsWith("]")) {
        inSection = line.substring(1, line.length() - 1).strip().equalsIgnoreCase(SECTION);
        continue;
      }
      int split = line.indexOf('=');
      if (!inSection || split < 0) {
        continue;
      }
      read.put(
          line.substring(0, split).strip().toLowerCase(Locale.ROOT),
          line.substring(split + 1).strip());
    }
    return read;
  }

  /**
   * The environment with the file's settings underneath it.
   *
   * <p>A name the environment already carries keeps its value, whatever the file says.
   */
  public static Map<String, String> overlay(Map<String, String> environment) {
    Map<String, String> fromFile = read(Paths.get("").toAbsolutePath());
    if (fromFile.isEmpty()) {
      return environment;
    }
    Map<String, String> merged = new LinkedHashMap<>(fromFile);
    merged.putAll(environment);
    return merged;
  }
}
