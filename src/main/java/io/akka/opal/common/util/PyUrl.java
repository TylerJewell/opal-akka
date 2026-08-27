package io.akka.opal.common.util;

import java.util.Locale;

/**
 * A URL split the way the source's own splitter splits one — SPEC-002 R277.
 *
 * <p>Not a general parser. It exists because two of OPAL's decisions read a URL that is not
 * always a URL: a policy repository may be written {@code git@host:org/repo.git}, which has no
 * scheme and no authority, and the strict parser in the platform library refuses it outright
 * rather than handing back the whole string as a path. The source's splitter never refuses, so a
 * webhook naming an ssh-style repository is answered rather than erroring.
 *
 * <p>The rules reproduced here: a scheme is the text before the first colon when that text is a
 * letter followed by letters, digits, {@code +}, {@code -} or {@code .}; an authority is present
 * only when what follows the scheme starts with {@code //}; the host is the authority with any
 * user information removed, lower-cased, and with the brackets of an IPv6 literal stripped.
 */
public record PyUrl(
    String scheme, String netloc, String path, String query, String fragment) {

  /** The host with no user information and no port, or null when there is no authority. */
  public String hostname() {
    if (netloc == null || netloc.isEmpty()) {
      return null;
    }
    String hostAndPort = netloc.substring(netloc.lastIndexOf('@') + 1);
    if (hostAndPort.startsWith("[")) {
      int close = hostAndPort.indexOf(']');
      return close < 0 ? null : hostAndPort.substring(1, close).toLowerCase(Locale.ROOT);
    }
    int colon = hostAndPort.indexOf(':');
    String host = colon < 0 ? hostAndPort : hostAndPort.substring(0, colon);
    return host.isEmpty() ? null : host.toLowerCase(Locale.ROOT);
  }

  /** The port, or null when the authority names none. */
  public Integer port() {
    if (netloc == null || netloc.isEmpty()) {
      return null;
    }
    String hostAndPort = netloc.substring(netloc.lastIndexOf('@') + 1);
    int colon =
        hostAndPort.startsWith("[")
            ? hostAndPort.indexOf(':', hostAndPort.indexOf(']'))
            : hostAndPort.indexOf(':');
    if (colon < 0 || colon == hostAndPort.length() - 1) {
      return null;
    }
    try {
      return Integer.valueOf(hostAndPort.substring(colon + 1));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * The schemes whose absent authority is still written as {@code //}.
   *
   * <p>The source's own list. It matters for {@code file:///path}, whose authority is empty and
   * whose reassembled form keeps all three slashes — without this a local repository url would
   * come back as {@code file:/path} and stop matching what a deployment configured.
   */
  private static final java.util.Set<String> USES_NETLOC =
      java.util.Set.of(
          "ftp", "http", "gopher", "nntp", "telnet", "imap", "wais", "file", "mms", "https",
          "shttp", "snews", "prospero", "rtsp", "rtspu", "rsync", "svn", "svn+ssh", "sftp", "nfs",
          "git", "git+ssh", "ws", "wss");

  public static PyUrl split(String url) {
    String rest = url == null ? "" : url;
    String scheme = "";
    int colon = rest.indexOf(':');
    if (colon > 0 && isScheme(rest.substring(0, colon))) {
      scheme = rest.substring(0, colon).toLowerCase(Locale.ROOT);
      rest = rest.substring(colon + 1);
    }
    String netloc = "";
    if (rest.startsWith("//")) {
      rest = rest.substring(2);
      int end = rest.length();
      for (int i = 0; i < rest.length(); i++) {
        char c = rest.charAt(i);
        if (c == '/' || c == '?' || c == '#') {
          end = i;
          break;
        }
      }
      netloc = rest.substring(0, end);
      rest = rest.substring(end);
    }
    String fragment = "";
    int hash = rest.indexOf('#');
    if (hash >= 0) {
      fragment = rest.substring(hash + 1);
      rest = rest.substring(0, hash);
    }
    String query = "";
    int question = rest.indexOf('?');
    if (question >= 0) {
      query = rest.substring(question + 1);
      rest = rest.substring(0, question);
    }
    return new PyUrl(scheme, netloc, rest, query, fragment);
  }

  /**
   * Reassembles the parts, the way the source's own {@code geturl} does.
   *
   * <p>An empty authority contributes no {@code //}, and an empty scheme contributes no colon —
   * which is what turns {@code git@host:org/repo.git} back into itself rather than into
   * something with a scheme of {@code git@host}.
   */
  public String geturl() {
    StringBuilder out = new StringBuilder();
    String body = path;
    boolean authorityExpected =
        scheme != null && !scheme.isEmpty() && USES_NETLOC.contains(scheme) && !body.startsWith("//");
    if ((netloc != null && !netloc.isEmpty()) || authorityExpected) {
      String authority = netloc == null ? "" : netloc;
      body = "//" + authority + (body.startsWith("/") || body.isEmpty() ? body : "/" + body);
    }
    if (scheme != null && !scheme.isEmpty()) {
      out.append(scheme).append(':');
    }
    out.append(body);
    if (query != null && !query.isEmpty()) {
      out.append('?').append(query);
    }
    if (fragment != null && !fragment.isEmpty()) {
      out.append('#').append(fragment);
    }
    return out.toString();
  }

  private static boolean isScheme(String candidate) {
    if (candidate.isEmpty() || !Character.isLetter(candidate.charAt(0))) {
      return false;
    }
    for (int i = 1; i < candidate.length(); i++) {
      char c = candidate.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '+' && c != '-' && c != '.') {
        return false;
      }
    }
    return true;
  }
}
