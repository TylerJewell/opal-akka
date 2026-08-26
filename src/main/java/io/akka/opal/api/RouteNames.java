package io.akka.opal.api;

import akka.javasdk.annotations.http.Delete;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Patch;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.annotations.http.Put;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The method and path template of the route currently answering — SPEC-002 R160.
 *
 * <p>A traced request needs the route it is on, and the route is written once, on the annotation
 * above the method. Reading it back from there rather than repeating it at every call site keeps
 * the two from drifting: a path edited in the annotation is the path the span reports.
 */
final class RouteNames {

  /** What a span calls a route: {@code GET} and {@code /statistics}. */
  record Route(String method, String path) {}

  private static final Route UNKNOWN = new Route("GET", "unknown");

  private static final Map<String, Route> CACHE = new ConcurrentHashMap<>();

  private RouteNames() {}

  static Route of(Class<?> endpoint, String methodName) {
    if (endpoint == null || methodName == null) {
      return UNKNOWN;
    }
    return CACHE.computeIfAbsent(
        endpoint.getName() + "#" + methodName, ignored -> resolve(endpoint, methodName));
  }

  private static Route resolve(Class<?> endpoint, String methodName) {
    HttpEndpoint prefix = endpoint.getAnnotation(HttpEndpoint.class);
    String base = prefix == null ? "" : prefix.value();
    for (Method method : endpoint.getDeclaredMethods()) {
      if (!method.getName().equals(methodName)) {
        continue;
      }
      Get get = method.getAnnotation(Get.class);
      if (get != null) {
        return new Route("GET", join(base, get.value()));
      }
      Post post = method.getAnnotation(Post.class);
      if (post != null) {
        return new Route("POST", join(base, post.value()));
      }
      Put put = method.getAnnotation(Put.class);
      if (put != null) {
        return new Route("PUT", join(base, put.value()));
      }
      Delete delete = method.getAnnotation(Delete.class);
      if (delete != null) {
        return new Route("DELETE", join(base, delete.value()));
      }
      Patch patch = method.getAnnotation(Patch.class);
      if (patch != null) {
        return new Route("PATCH", join(base, patch.value()));
      }
    }
    return UNKNOWN;
  }

  static String join(String base, String path) {
    String whole = (base == null ? "" : base) + (path == null ? "" : path);
    return whole.isEmpty() ? "/" : whole;
  }
}
