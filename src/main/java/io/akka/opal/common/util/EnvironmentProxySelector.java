package io.akka.opal.common.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The egress proxy the environment names — SPEC-002 R291.
 *
 * <p>Every outbound call the source makes goes through a client built with {@code trust_env=True},
 * which reads {@code HTTP_PROXY}, {@code HTTPS_PROXY} and {@code NO_PROXY}. A deployment behind an
 * egress proxy reaches its policy engine, its OPAL server, its data sources and its callbacks only
 * because of that, so the same variables are read here.
 *
 * <p>Both spellings are accepted, lower case first, which is what the library underneath the
 * source does.
 */
public final class EnvironmentProxySelector extends ProxySelector {

  private final List<String> noProxy;
  private final Proxy httpProxy;
  private final Proxy httpsProxy;

  public EnvironmentProxySelector() {
    this(System.getenv());
  }

  EnvironmentProxySelector(java.util.Map<String, String> environment) {
    this.httpProxy = proxyFrom(value(environment, "http_proxy", "HTTP_PROXY"));
    this.httpsProxy = proxyFrom(value(environment, "https_proxy", "HTTPS_PROXY"));
    this.noProxy = hosts(value(environment, "no_proxy", "NO_PROXY"));
  }

  /** Whether anything at all was configured, so a process with no proxy keeps the default. */
  public boolean configured() {
    return httpProxy != null || httpsProxy != null;
  }

  @Override
  public List<Proxy> select(URI uri) {
    Proxy chosen = "https".equalsIgnoreCase(uri.getScheme()) ? httpsProxy : httpProxy;
    if (chosen == null || bypassed(uri.getHost())) {
      return List.of(Proxy.NO_PROXY);
    }
    return List.of(chosen);
  }

  @Override
  public void connectFailed(URI uri, SocketAddress address, IOException failure) {
    // Nothing to fall back to: the environment names one proxy and a failure to reach it is the
    // caller's to see.
  }

  /**
   * Whether the host is one {@code NO_PROXY} exempts.
   *
   * <p>{@code *} exempts everything, and an entry matches a host that equals it or ends with it
   * after a dot — the rule the library underneath the source uses.
   */
  boolean bypassed(String host) {
    if (host == null) {
      return false;
    }
    String candidate = host.toLowerCase(Locale.ROOT);
    for (String entry : noProxy) {
      if (entry.equals("*")) {
        return true;
      }
      String bare = entry.startsWith(".") ? entry.substring(1) : entry;
      if (candidate.equals(bare) || candidate.endsWith("." + bare)) {
        return true;
      }
    }
    return false;
  }

  private static String value(java.util.Map<String, String> environment, String lower, String upper) {
    String found = environment.get(lower);
    return found != null && !found.isBlank() ? found : environment.get(upper);
  }

  private static List<String> hosts(String raw) {
    List<String> entries = new ArrayList<>();
    if (raw == null || raw.isBlank()) {
      return entries;
    }
    for (String part : raw.split(",")) {
      String trimmed = part.trim().toLowerCase(Locale.ROOT);
      if (!trimmed.isEmpty()) {
        entries.add(trimmed);
      }
    }
    return entries;
  }

  private static Proxy proxyFrom(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    String text = raw.trim();
    if (!text.contains("://")) {
      text = "http://" + text;
    }
    try {
      URI parsed = new URI(text);
      String host = parsed.getHost();
      if (host == null) {
        return null;
      }
      int port = parsed.getPort() > 0 ? parsed.getPort() : 80;
      return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(host, port));
    } catch (Exception e) {
      return null;
    }
  }
}
