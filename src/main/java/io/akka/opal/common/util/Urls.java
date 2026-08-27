package io.akka.opal.common.util;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Making a URL safe to write down, and setting a parameter on one.
 *
 * <p>A data source URL and a policy repository URL routinely carry a credential — either as
 * {@code user:token@host} or as a query parameter — and both are interpolated into log lines and
 * into the transaction log. Redaction keeps the host, the port, the path and the parameters that
 * are not secret, so a line is still worth reading after it has been made safe.
 *
 * <p>This never raises. It is called from log and exception paths, so a URL it cannot parse comes
 * back unchanged rather than taking the log line with it.
 */
public final class Urls {

  /** The parameter names whose values may be a credential. */
  static final Set<String> SENSITIVE_QUERY_PARAMS =
      Set.of(
          "token",
          "access_token",
          "api_key",
          "apikey",
          "key",
          "password",
          "secret",
          "sig",
          "signature");

  private static final Pattern USERINFO =
      Pattern.compile("(?<scheme>[a-zA-Z][a-zA-Z0-9+.\\-]*://)[^/@\\s]+@");

  private Urls() {}

  public static String redactUrl(String url) {
    if (url == null || url.isEmpty()) {
      return url;
    }
    // Split the permissive way rather than through the platform's URI type. That type refuses a
    // host holding an underscore outright, and a redaction that gives up on one hands the
    // credentials it was asked to remove straight to a log.
    PyUrl parts = PyUrl.split(url);
    if (parts.scheme() == null || parts.scheme().isEmpty()) {
      return url;
    }

    boolean changed = false;
    String authority = parts.netloc();
    if (authority != null && authority.contains("@")) {
      String hostAndPort = authority.substring(authority.lastIndexOf('@') + 1);
      if (portOutOfRange(hostAndPort)) {
        // Never throw from a log path: an unreadable authority is returned as it arrived, the
        // way the source's own lazy parse bails out.
        return url;
      }
      authority = "***@" + hostAndPort;
      changed = true;
    }

    String[] query = maskSensitiveParams(parts.query());
    String[] fragment = maskSensitiveParams(parts.fragment());
    changed = changed || query[1] != null || fragment[1] != null;
    if (!changed) {
      return url;
    }
    StringBuilder out = new StringBuilder(parts.scheme()).append("://");
    if (authority != null) {
      out.append(authority);
    }
    if (parts.path() != null) {
      out.append(parts.path());
    }
    if (query[0] != null && !query[0].isEmpty()) {
      out.append('?').append(query[0]);
    }
    if (fragment[0] != null && !fragment[0].isEmpty()) {
      out.append('#').append(fragment[0]);
    }
    return out.toString();
  }

  /** Whether the trailing {@code :digits} of an authority names a port no socket can carry. */
  private static boolean portOutOfRange(String hostAndPort) {
    int colon = hostAndPort.lastIndexOf(':');
    if (colon < 0 || colon < hostAndPort.lastIndexOf(']')) {
      return false;
    }
    String port = hostAndPort.substring(colon + 1);
    if (port.isEmpty()) {
      return false;
    }
    for (int i = 0; i < port.length(); i++) {
      if (!Character.isDigit(port.charAt(i))) {
        return true;
      }
    }
    try {
      int value = Integer.parseInt(port);
      return value < 0 || value > 65535;
    } catch (NumberFormatException e) {
      return true;
    }
  }

  /** Returns the rewritten component and, in the second slot, a marker when it changed. */
  private static String[] maskSensitiveParams(String component) {
    if (component == null || component.isEmpty()) {
      return new String[] {component, null};
    }
    boolean changed = false;
    List<String> out = new ArrayList<>();
    for (String pair : component.split("&", -1)) {
      int equals = pair.indexOf('=');
      if (equals >= 0
          && SENSITIVE_QUERY_PARAMS.contains(
              pair.substring(0, equals).toLowerCase(Locale.ROOT))) {
        out.add(pair.substring(0, equals) + "=***");
        changed = true;
      } else {
        out.add(pair);
      }
    }
    return new String[] {String.join("&", out), changed ? "changed" : null};
  }

  /**
   * Free text — a git error, an exception message — with a known URL replaced by its redacted
   * form and any other {@code scheme://user:pass@} scrubbed. The known URL goes first: the regex
   * would otherwise rewrite the credentials out from under the verbatim match and leave that
   * URL's query token in place.
   */
  public static String redactUrlInText(String text, String url) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    String scrubbed = text;
    if (url != null && !url.isEmpty()) {
      scrubbed = scrubbed.replace(url, redactUrl(url));
    }
    Matcher matcher = USERINFO.matcher(scrubbed);
    return matcher.replaceAll(match -> Matcher.quoteReplacement(match.group("scheme") + "***@"));
  }

  /** Sets, or replaces, one query parameter. */
  public static String setUrlQueryParam(String url, String name, String value) {
    URI parsed;
    try {
      parsed = new URI(url);
    } catch (Exception e) {
      return url;
    }
    Map<String, String> params = new LinkedHashMap<>();
    String rawQuery = parsed.getRawQuery();
    if (rawQuery != null && !rawQuery.isEmpty()) {
      for (String pair : rawQuery.split("&")) {
        int equals = pair.indexOf('=');
        if (equals >= 0) {
          params.put(pair.substring(0, equals), pair.substring(equals + 1));
        } else {
          params.put(pair, "");
        }
      }
    }
    params.put(name, URLEncoder.encode(value, StandardCharsets.UTF_8));

    List<String> pairs = new ArrayList<>();
    params.forEach((key, item) -> pairs.add(key + "=" + item));
    StringBuilder out = new StringBuilder();
    if (parsed.getScheme() != null) {
      out.append(parsed.getScheme()).append("://");
    }
    if (parsed.getRawAuthority() != null) {
      out.append(parsed.getRawAuthority());
    }
    if (parsed.getRawPath() != null) {
      out.append(parsed.getRawPath());
    }
    if (!pairs.isEmpty()) {
      out.append('?').append(String.join("&", pairs));
    }
    if (parsed.getRawFragment() != null && !parsed.getRawFragment().isEmpty()) {
      out.append('#').append(parsed.getRawFragment());
    }
    return out.toString();
  }
}
