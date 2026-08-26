package io.akka.opal.api;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCode;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.RawHeader;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.monitoring.Apm;
import io.akka.opal.common.monitoring.Span;
import io.akka.opal.server.pubsub.Rpc;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The response shapes OPAL's own web framework produces, reproduced so a caller written against
 * the original does not have to change.
 *
 * <p>The envelope matters as much as the status: an error is {@code {"detail": ...}} where the
 * detail is a string for a plain failure and an object for one carrying a reason, and a 401
 * additionally answers {@code WWW-Authenticate: Bearer}. Clients branch on those.
 */
public final class Responses {

  private Responses() {}

  /**
   * R129: the origins CORS allows, read once at start-up.
   *
   * <p>Held here rather than passed in because every response carries these headers, including
   * the ones produced where no configuration is in scope — a 404 for a route belonging to the
   * other role, or a 500 nothing planned for. A browser that gets the headers on the success and
   * not on the error reports the error as a CORS failure and hides what it was.
   */
  private static volatile List<String> allowedOrigins = List.of("*");

  public static void configureCors(List<String> origins) {
    allowedOrigins = origins == null || origins.isEmpty() ? List.of("*") : origins;
  }

  static List<String> allowedOrigins() {
    return allowedOrigins;
  }

  /**
   * R264: the CORS headers, which depend on who asked.
   *
   * <p>Three rules a browser enforces and a wildcard does not satisfy. A request carrying cookies
   * may not be answered with {@code *}, so the origin is mirrored. A deployment that named its
   * origins mirrors the one that asked, and adds {@code Vary: Origin} so a cache does not serve
   * one origin's answer to another. And an origin that is not allowed gets no CORS headers at
   * all, which is how a browser is told no.
   */
  private static HttpResponse withCors(HttpResponse response) {
    Caller caller = CALLER.get();
    String origin = caller == null ? null : caller.origin();
    boolean allowAll = allowedOrigins.contains("*");
    String allowOrigin;
    boolean vary = false;
    if (allowAll) {
      allowOrigin =
          origin != null && caller.hasCookie() ? origin : String.join(", ", allowedOrigins);
    } else if (origin != null && allowedOrigins.contains(origin)) {
      allowOrigin = origin;
      vary = true;
    } else if (origin != null) {
      return response;
    } else {
      allowOrigin = String.join(", ", allowedOrigins);
    }
    List<HttpHeader> headers =
        new java.util.ArrayList<>(
            List.of(
                RawHeader.create("Access-Control-Allow-Origin", allowOrigin),
                RawHeader.create("Access-Control-Allow-Credentials", "true"),
                RawHeader.create("Access-Control-Allow-Methods", "*"),
                RawHeader.create("Access-Control-Allow-Headers", "*")));
    if (vary) {
      headers.add(RawHeader.create("Vary", "Origin"));
    }
    return response.addHeaders(headers);
  }

  /** What the current request said about who is asking. */
  record Caller(String origin, boolean hasCookie) {}

  private static final ThreadLocal<Caller> CALLER = new ThreadLocal<>();

  /** Made visible so a test can put a caller in place without a running server. */
  static void setCaller(Caller caller) {
    CALLER.set(caller);
  }

  static void clearCaller() {
    CALLER.remove();
  }

  public static HttpResponse json(StatusCode status, Object body) {
    byte[] bytes;
    try {
      bytes = Rpc.MAPPER.writeValueAsBytes(body);
    } catch (Exception e) {
      bytes = "{}".getBytes(StandardCharsets.UTF_8);
    }
    return withCors(
        HttpResponse.create().withStatus(status).withEntity(ContentTypes.APPLICATION_JSON, bytes));
  }

  public static HttpResponse ok(Object body) {
    return json(StatusCodes.OK, body);
  }

  /** {@code {"detail": <text>}} — what the framework answers for an ordinary failure. */
  public static HttpResponse detail(StatusCode status, String text) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("detail", text);
    return json(status, body);
  }

  /** {@code {"detail": {...}}} — what a failure carrying structure answers. */
  public static HttpResponse detail(StatusCode status, Map<String, Object> object) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("detail", object);
    return json(status, body);
  }

  /** R71: the reason, the token that failed, and the challenge header. */
  public static HttpResponse unauthorized(Unauthorized failure) {
    List<HttpHeader> headers = List.of(RawHeader.create("WWW-Authenticate", "Bearer"));
    return detail(StatusCodes.UNAUTHORIZED, failure.detail()).addHeaders(headers);
  }

  public static HttpResponse notFound() {
    return detail(StatusCodes.NOT_FOUND, "Not Found");
  }

  /** R130: an uncaught failure, in the shape the source's own handler produces. */
  public static HttpResponse uncaught() {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("error", "Uncaught server exception");
    return json(StatusCodes.INTERNAL_SERVER_ERROR, body);
  }

  /**
   * R130: one route's answer, with anything it did not plan for turned into that shape.
   *
   * <p>The source's web framework installs a single handler around every route. This target has
   * no equivalent hook, so the guard is written at each route instead — and it has to be at every
   * one of them, because a caller cannot tell which route it is talking to from the shape of the
   * failure, and half a policy is worse than none.
   *
   * <p>The failure is logged with its stack trace and answered without it: the message may carry
   * whatever the caller sent, and the answer goes back to the caller.
   */
  /**
   * R264: the same guard, told who is asking.
   *
   * <p>Every route calls this shape, because the CORS headers on the answer depend on the
   * request's own origin and a static helper has no other way to see it.
   */
  public static HttpResponse guarded(
      akka.javasdk.http.RequestContext context, java.util.function.Supplier<HttpResponse> route) {
    setCaller(
        new Caller(
            context.requestHeader("Origin").map(akka.http.javadsl.model.HttpHeader::value)
                .orElse(null),
            context.requestHeader("Cookie").isPresent()));
    try {
      return guarded(route);
    } finally {
      clearCaller();
    }
  }

  public static HttpResponse guarded(java.util.function.Supplier<HttpResponse> route) {
    if (!Apm.enabled()) {
      return answer(route);
    }
    RouteNames.Route named = RouteNames.of(callerClass(), callerMethod());
    try (Span span = Apm.httpSpan(named.method(), named.path())) {
      HttpResponse response = answer(route);
      if (span != null) {
        span.setTag("http.status_code", String.valueOf(response.status().intValue()));
        if (response.status().intValue() >= 500) {
          span.setError();
        }
      }
      return response;
    }
  }

  private static HttpResponse answer(java.util.function.Supplier<HttpResponse> route) {
    try {
      return route.get();
    } catch (Exception e) {
      LOG.error("Uncaught server exception", e);
      return uncaught();
    }
  }

  /**
   * The endpoint class and method that called {@link #guarded}.
   *
   * <p>Walked rather than passed in: the route template already exists on the annotation above
   * that method, and a span that reads it there cannot disagree with the path actually served.
   * Only walked while tracing is on, which is off by default on both sides.
   */
  private static final StackWalker WALKER =
      StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

  private static Class<?> callerClass() {
    StackWalker.StackFrame frame = frame();
    return frame == null ? null : frame.getDeclaringClass();
  }

  private static String callerMethod() {
    StackWalker.StackFrame frame = frame();
    return frame == null ? null : frame.getMethodName();
  }

  private static StackWalker.StackFrame frame() {
    return WALKER.walk(
        frames ->
            frames
                .filter(f -> !f.getDeclaringClass().equals(Responses.class))
                .findFirst()
                .orElse(null));
  }

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(Responses.class);

  public static HttpResponse html(String markup) {
    return withCors(
        HttpResponse.create()
            .withStatus(StatusCodes.OK)
            .withEntity(ContentTypes.TEXT_HTML_UTF8, markup.getBytes(StandardCharsets.UTF_8)));
  }

  public static HttpResponse redirect(String location) {
    return withCors(
        HttpResponse.create()
            .withStatus(StatusCodes.TEMPORARY_REDIRECT)
            .addHeader(RawHeader.create("Location", location)));
  }

  public static HttpResponse noContent() {
    return withCors(HttpResponse.create().withStatus(StatusCodes.NO_CONTENT));
  }

  public static HttpResponse created() {
    return withCors(HttpResponse.create().withStatus(StatusCodes.CREATED));
  }

  /** {@code {"status": "ok"}}, which several routes answer and nothing else. */
  public static HttpResponse statusOk() {
    return ok(Map.of("status", "ok"));
  }
}
