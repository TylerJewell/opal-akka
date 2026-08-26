package io.akka.opal.api;

import io.akka.opal.Role;
import java.util.ArrayList;
import java.util.List;

/**
 * The route table each role mounts — SPEC-002 R127.
 *
 * <p>Declared rather than derived because it is a claim about the surface: the census test
 * compares this list against the one taken off the running original, and the OpenAPI document is
 * built from it, so a route added to the code and not to this list shows up as a difference
 * rather than as silence.
 */
public final class Routes {

  /** One route: its path, the methods it answers, and the name the source's own table gives it. */
  public record Route(String path, List<String> methods, String name) {}

  private Routes() {}

  /** The 19 the source's server mounts, in its own order. */
  public static final List<Route> SERVER =
      List.of(
          new Route("/openapi.json", List.of("GET", "HEAD"), "openapi"),
          new Route("/docs", List.of("GET", "HEAD"), "swagger_ui_html"),
          new Route("/docs/oauth2-redirect", List.of("GET", "HEAD"), "swagger_ui_redirect"),
          new Route("/policy", List.of("GET"), "get_policy"),
          new Route("/policy-data", List.of("GET"), "default_all_data"),
          new Route("/data/callback_report", List.of("POST"), "log_client_update_report"),
          new Route("/data/config", List.of("GET"), "get_data_sources_config"),
          new Route("/data/config", List.of("POST"), "publish_data_update_event"),
          new Route("/webhook", List.of("POST"), "trigger_webhook"),
          new Route("/token", List.of("POST"), "generate_new_access_token"),
          new Route("/ws", List.of(), "websocket_rpc_endpoint"),
          new Route("/pubsub_client_info", List.of("GET"), "client_info"),
          new Route("/statistics", List.of("GET"), "get_statistics"),
          new Route("/stats", List.of("GET"), "get_stat_counts"),
          new Route("/loadlimit", List.of("GET"), "loadlimit"),
          new Route("/.well-known", List.of(), "jwks_dir"),
          new Route("/redoc", List.of("GET"), "redoc_html"),
          new Route("/", List.of("GET"), "root"),
          new Route("/healthcheck", List.of("GET"), "healthcheck"));

  /** The nine {@code SCOPES} adds, which land between {@code /loadlimit} and {@code /.well-known}. */
  public static final List<Route> SCOPES =
      List.of(
          new Route("/scopes", List.of("PUT"), "put_scope"),
          new Route("/scopes", List.of("GET"), "get_all_scopes"),
          new Route("/scopes/{scope_id}", List.of("GET"), "get_scope"),
          new Route("/scopes/{scope_id}", List.of("DELETE"), "delete_scope"),
          new Route("/scopes/{scope_id}/refresh", List.of("POST"), "refresh_scope"),
          new Route("/scopes/refresh", List.of("POST"), "sync_all_scopes"),
          new Route("/scopes/{scope_id}/policy", List.of("GET"), "get_scope_policy"),
          new Route("/scopes/{scope_id}/data", List.of("GET"), "get_scope_data_config"),
          new Route("/scopes/{scope_id}/data/update", List.of("POST"), "publish_data_update_event"));

  /** The 18 the source's client mounts, in its own order. */
  public static final List<Route> CLIENT =
      List.of(
          new Route("/openapi.json", List.of("GET", "HEAD"), "openapi"),
          new Route("/docs", List.of("GET", "HEAD"), "swagger_ui_html"),
          new Route("/docs/oauth2-redirect", List.of("GET", "HEAD"), "swagger_ui_redirect"),
          new Route("/redoc", List.of("GET", "HEAD"), "redoc_html"),
          new Route("/policy-updater/trigger", List.of("POST"), "trigger_policy_update"),
          new Route("/data-updater/trigger", List.of("POST"), "trigger_policy_data_update"),
          new Route("/policy-store/config", List.of("GET"), "get_policy_store_details"),
          new Route("/callbacks", List.of("GET"), "list_callbacks"),
          new Route("/callbacks/{key}", List.of("GET"), "get_callback_by_key"),
          new Route("/callbacks", List.of("POST"), "register_callback"),
          new Route("/callbacks/{key}", List.of("DELETE"), "get_callback_by_key"),
          new Route("/opal-server/connectivity", List.of("GET"), "get_connectivity_status"),
          new Route("/opal-server/connectivity/disable", List.of("POST"), "disable_connectivity"),
          new Route("/opal-server/connectivity/enable", List.of("POST"), "enable_connectivity"),
          new Route("/healthy", List.of("GET"), "healthy"),
          new Route("/", List.of("GET"), "healthy"),
          new Route("/healthcheck", List.of("GET"), "healthy"),
          new Route("/ready", List.of("GET"), "ready"));

  /** The debug route, present only with {@code DEBUG_INTERNAL_STATS} on. */
  public static final Route INTERNAL_STATS =
      new Route("/internal/git-fetcher-cache-stats", List.of("GET"), "git_fetcher_cache_stats");

  /** This port's own, where OPAL's vendor-agent measurements are readable — OD-9. */
  public static final Route INTERNAL_METRICS =
      new Route("/internal/metrics", List.of("GET"), "metrics");

  public static List<Route> serverRoutes(boolean scopesEnabled, boolean debugInternalStats) {
    List<Route> routes = new ArrayList<>();
    for (Route route : SERVER) {
      if (scopesEnabled && route.path().equals("/.well-known")) {
        routes.addAll(SCOPES);
      }
      routes.add(route);
    }
    if (debugInternalStats) {
      routes.add(INTERNAL_STATS);
    }
    return routes;
  }

  public static List<Route> mounted(boolean scopesEnabled, boolean debugInternalStats) {
    List<Route> routes = new ArrayList<>();
    if (Role.isServer()) {
      routes.addAll(serverRoutes(scopesEnabled, debugInternalStats));
      routes.add(INTERNAL_METRICS);
    }
    if (Role.isClient()) {
      for (Route route : CLIENT) {
        if (!routes.contains(route)) {
          routes.add(route);
        }
      }
    }
    return routes;
  }
}
