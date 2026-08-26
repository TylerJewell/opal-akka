package io.akka.opal.cli;

import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.common.confi.Confi;
import io.akka.opal.common.confi.ConfiEntry;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Security;
import io.akka.opal.server.config.ServerConfig;
import io.akka.opal.server.pubsub.Rpc;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * OPAL's command line — SPEC-002 R134 to R139.
 *
 * <p>Six commands on each of the two processes, plus every configuration entry as a global option.
 * The option set is not written out here: it is derived from the same configuration objects the
 * running service reads, so a new entry becomes a new option without anybody remembering to add
 * it — which is how the source does it too.
 */
public final class OpalCli {

  /** Which of the two commands this invocation is. */
  public enum Which {
    server("💎 OPAL-SERVER 💎", "server"),
    client("OPAL-CLIENT", "client");

    final String banner;
    final String name;

    Which(String banner, String name) {
      this.banner = banner;
      this.name = name;
    }
  }

  private final Which which;
  private final Map<String, String> environment;
  private final PrintStream out;

  public OpalCli(Which which, Map<String, String> environment, PrintStream out) {
    this.which = which;
    this.environment = new HashMap<>(environment);
    this.out = out;
  }

  public static void main(String[] args) {
    Which which =
        args.length > 0 && args[0].equals("--client")
            ? Which.client
            : io.akka.opal.Role.isClient() && !io.akka.opal.Role.isServer()
                ? Which.client
                : Which.server;
    String[] rest = args.length > 0 && args[0].startsWith("--client") ? tail(args) : args;
    int code = new OpalCli(which, System.getenv(), System.out).run(rest);
    System.exit(code);
  }

  private static String[] tail(String[] args) {
    return Arrays.copyOfRange(args, 1, args.length);
  }

  /** Returns the process exit code. */
  public int run(String[] args) {
    List<String> arguments = new ArrayList<>(Arrays.asList(args));
    Map<String, String> overrides = new LinkedHashMap<>();
    String command = extractCommand(arguments, overrides);

    Confi own = which == Which.server ? new ServerConfig(environment) : new ClientConfig(environment);
    CommonConfig common = new CommonConfig(environment);
    overrides.forEach(
        (key, value) -> {
          if (!own.applyCommandLine(key, value)) {
            common.applyCommandLine(key, value);
          }
        });

    if (command == null) {
      // R139: no subcommand prints the banner, the usage and the configuration help, and exits 0.
      out.println(banner());
      out.println(usage(own, common));
      out.println(docs());
      return 0;
    }

    return switch (command) {
      case "run" -> run(arguments);
      case "print-config" -> printConfig(own, common);
      case "obtain-token" -> obtainToken(arguments);
      case "generate-secret" -> generateSecret(arguments);
      case "publish-data-update" -> publishDataUpdate(arguments);
      case "version" -> {
        out.println(version());
        yield 0;
      }
      default -> {
        out.println("no such command: " + command);
        yield 2;
      }
    };
  }

  /** Pulls the subcommand out, leaving its own arguments and collecting the global options. */
  private String extractCommand(List<String> arguments, Map<String, String> overrides) {
    List<String> commands =
        List.of("run", "print-config", "obtain-token", "generate-secret", "publish-data-update",
            "version");
    for (int i = 0; i < arguments.size(); i++) {
      String argument = arguments.get(i);
      if (commands.contains(argument)) {
        arguments.remove(i);
        return argument;
      }
      if (argument.startsWith("--") && argument.contains("=")) {
        int equals = argument.indexOf('=');
        String option = argument.substring(2, equals);
        String value = argument.substring(equals + 1);
        String key = optionToKey(option);
        if (key != null) {
          overrides.put(key, value);
          arguments.remove(i--);
        }
      }
    }
    return null;
  }

  /** R11: the option name is the key lowercased with hyphens; this is the way back. */
  static String optionToKey(String option) {
    return option.toUpperCase(java.util.Locale.ROOT).replace('-', '_');
  }

  // -- the six commands ----------------------------------------------------

  private int run(List<String> arguments) {
    String engineType = optionValue(arguments, "--engine-type", "uvicron");
    out.println(banner());
    out.println(
        which == Which.server
            ? "-- Starting OPAL Server (with " + engineType + ") --"
            : "-- Starting OPAL client (with " + engineType + ") --");
    out.println(
        "This build runs as an Akka service; start it with `mvn compile exec:java` or the"
            + " container image, and set OPAL_ROLE to choose the role.");
    return 0;
  }

  /** R10: the process's own entries, then the shared ones, each as sorted JSON. */
  private int printConfig(Confi own, CommonConfig common) {
    out.println("Printing configuration values");
    out.println(own.printConfig());
    out.println(common.printConfig());
    return 0;
  }

  /** R136: post an access-token request, and print only the token unless asked for all of it. */
  private int obtainToken(List<String> arguments) {
    String masterToken = firstPositional(arguments);
    String serverUrl = optionValue(arguments, "--server-url", "http://localhost:7002");
    String type = optionValue(arguments, "--type", "client");
    String claims = optionValue(arguments, "--claims", "{}");
    boolean justTheToken = !arguments.contains("--no-just-the-token");
    Duration ttl = readTtl(arguments);

    try {
      Map<String, Object> request = new LinkedHashMap<>();
      request.put("id", UUID.randomUUID().toString());
      request.put("type", type);
      request.put("ttl", ttl.toSeconds());
      request.put("claims", Rpc.MAPPER.readTree(claims));
      HttpResponse<String> response =
          io.akka.opal.common.util.Http.plain()
              .send(
                  HttpRequest.newBuilder(URI.create(serverUrl + "/token"))
                      .header("Authorization", "bearer " + masterToken)
                      .header("content-type", "application/json")
                      .POST(
                          HttpRequest.BodyPublishers.ofString(
                              Rpc.MAPPER.writeValueAsString(request), StandardCharsets.UTF_8))
                      .build(),
                  HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      var body = Rpc.MAPPER.readTree(response.body());
      out.println(justTheToken ? body.path("token").asText() : body.toString());
      return 0;
    } catch (Exception e) {
      out.println("could not obtain a token: " + e);
      return 1;
    }
  }

  /** {@code --ttl 365 days}, which is a number and a unit rather than one word. */
  static Duration readTtl(List<String> arguments) {
    int index = arguments.indexOf("--ttl");
    if (index < 0 || index + 2 >= arguments.size()) {
      return Duration.ofDays(365);
    }
    long amount = Long.parseLong(arguments.get(index + 1));
    String unit = arguments.get(index + 2);
    return switch (unit) {
      case "milliseconds" -> Duration.ofMillis(amount);
      case "seconds" -> Duration.ofSeconds(amount);
      case "minutes" -> Duration.ofMinutes(amount);
      case "hours" -> Duration.ofHours(amount);
      case "weeks" -> Duration.ofDays(amount * 7);
      default -> Duration.ofDays(amount);
    };
  }

  /** R135: URL-safe by default, hex or a Python bytes repr when asked. */
  private int generateSecret(List<String> arguments) {
    int size = Integer.parseInt(optionValue(arguments, "--size", "32"));
    String format = optionValue(arguments, "--format", "urlsafe");
    byte[] bytes = new byte[size];
    new SecureRandom().nextBytes(bytes);
    out.println(
        switch (format) {
          case "hex" -> io.akka.opal.common.util.Hashing.hex(bytes);
          case "bytes" -> pythonBytesRepr(bytes);
          default -> Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        });
    return 0;
  }

  /** {@code b'\\x00...'} — the form Python's {@code repr} of a bytes object takes. */
  static String pythonBytesRepr(byte[] bytes) {
    StringBuilder out = new StringBuilder("b'");
    for (byte b : bytes) {
      int value = b & 0xFF;
      if (value == '\'') {
        out.append("\\'");
      } else if (value == '\\') {
        out.append("\\\\");
      } else if (value == '\n') {
        out.append("\\n");
      } else if (value == '\r') {
        out.append("\\r");
      } else if (value == '\t') {
        out.append("\\t");
      } else if (value >= 0x20 && value < 0x7f) {
        out.append((char) value);
      } else {
        out.append(String.format("\\x%02x", value));
      }
    }
    return out.append('\'').toString();
  }

  /** R137: a single entry from the single-entry options, or the JSON list, and never both. */
  private int publishDataUpdate(List<String> arguments) {
    String token = firstPositional(arguments);
    String serverUrl = optionValue(arguments, "--server-url", "http://localhost:7002");
    String serverRoute = optionValue(arguments, "--server-route", "/data/config");
    String reason = optionValue(arguments, "--reason", "");
    String entriesJson = optionValue(arguments, "--entries", "[]");
    String srcUrl = optionValue(arguments, "--src-url", null);
    String data = optionValue(arguments, "--data", null);
    String srcConfig = optionValue(arguments, "--src-config", "{}");
    String dstPath = optionValue(arguments, "--dst-path", "");
    String saveMethod = optionValue(arguments, "--save-method", "PUT");
    List<String> topics = allOptionValues(arguments, "--topic");

    try {
      List<Data.DataSourceEntry> entries;
      boolean hasEntries = !entriesJson.equals("[]") && !entriesJson.isBlank();
      if (!hasEntries && srcUrl == null) {
        out.println(
            "You must provide either multiple entries (-e / --entries) or a single entry update"
                + " (--src_url)");
        return 0;
      }
      if (srcUrl != null) {
        entries =
            List.of(
                new Data.DataSourceEntry(
                    srcUrl,
                    Rpc.MAPPER.convertValue(Rpc.MAPPER.readTree(srcConfig), Map.class),
                    topics.isEmpty() ? null : topics,
                    dstPath,
                    saveMethod,
                    data == null ? null : Rpc.MAPPER.readTree(data),
                    null));
      } else {
        entries =
            Arrays.asList(
                Rpc.MAPPER.readValue(entriesJson, Data.DataSourceEntry[].class));
      }
      Data.DataUpdate update = new Data.DataUpdate(null, entries, reason, null);
      out.println("Publishing event:");
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(serverUrl + serverRoute))
              .header("content-type", "application/json")
              .POST(
                  HttpRequest.BodyPublishers.ofString(
                      Rpc.MAPPER.writeValueAsString(update), StandardCharsets.UTF_8));
      if (token != null) {
        request.header("Authorization", "bearer " + token);
      }
      HttpResponse<String> response =
          io.akka.opal.common.util.Http.plain()
              .send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      if (response.statusCode() == 200) {
        out.println("Event Published Successfully");
      } else {
        out.println("Event publishing failed with status-code - {res.status}");
        out.println(response.body());
      }
      return 0;
    } catch (Exception e) {
      out.println("could not publish the update: " + e);
      return 1;
    }
  }

  /** R138: the version of the common package. */
  public static String version() {
    return "0.0.0";
  }

  // -- help ----------------------------------------------------------------

  /**
   * The banner, including the two control characters the source puts in it.
   *
   * <p>The backspace and form feed are not stray escapes: the source's help text is written for
   * click, which reads a backspace as "do not re-wrap the paragraph that follows" and a form feed
   * as the end of the short help. They are invisible in a terminal and they are part of what
   * {@code --help} emits, so a caller diffing the two outputs sees them.
   */
  String banner() {
    return "\b\n    "
        + which.banner
        + "\n    Open-Policy Administration Layer - "
        + which.name
        + "\b\f";
  }

  String usage(Confi own, CommonConfig common) {
    StringBuilder text = new StringBuilder();
    text.append("Usage: opal-").append(which.name).append(" [OPTIONS] COMMAND [ARGS]...\n\n");
    text.append("Commands:\n");
    for (String command :
        List.of("run", "print-config", "obtain-token", "generate-secret", "publish-data-update",
            "version")) {
      text.append("  ").append(command).append('\n');
    }
    text.append("\nOptions:\n");
    for (ConfiEntry<?> entry : own.entries().values()) {
      text.append("  ").append(entry.optionName()).append('\n');
    }
    for (ConfiEntry<?> entry : common.entries().values()) {
      text.append("  ").append(entry.optionName()).append('\n');
    }
    return text.toString();
  }

  String docs() {
    return "\b\n"
        + "    Config top level options:\n"
        + "        - Use env-vars (same as cmd options) but uppercase\n"
        + "            and with \"_\" instead of \"-\"; all prefixed with \"OPAL_\"\n"
        + "        - Use command line options as detailed by '--help'\n"
        + "        - Use .env or .ini files\n\n"
        + "    \b\n"
        + "    Examples:\n"
        + "        - opal-" + which.name + " --help                           Detailed help on CLI\n"
        + "        - opal-" + which.name + " run --help                       Help on run command\n"
        + "        - opal-" + which.name + " run --engine-type gunicorn       Run " + which.name
        + " with gunicorn\n"
        + "    \b\n    ";
  }

  // -- argument reading ----------------------------------------------------

  private static String optionValue(List<String> arguments, String option, String fallback) {
    int index = arguments.indexOf(option);
    if (index >= 0 && index + 1 < arguments.size()) {
      return arguments.get(index + 1);
    }
    for (String argument : arguments) {
      if (argument.startsWith(option + "=")) {
        return argument.substring(option.length() + 1);
      }
    }
    return fallback;
  }

  private static List<String> allOptionValues(List<String> arguments, String option) {
    List<String> values = new ArrayList<>();
    for (int i = 0; i < arguments.size(); i++) {
      if (arguments.get(i).equals(option) && i + 1 < arguments.size()) {
        values.add(arguments.get(i + 1));
      } else if (arguments.get(i).startsWith(option + "=")) {
        values.add(arguments.get(i).substring(option.length() + 1));
      }
    }
    return values;
  }

  private static String firstPositional(List<String> arguments) {
    for (int i = 0; i < arguments.size(); i++) {
      String argument = arguments.get(i);
      if (argument.startsWith("-")) {
        i++;
        continue;
      }
      return argument;
    }
    return null;
  }
}
