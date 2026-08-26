package io.akka.opal.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.http.javadsl.model.HttpResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.SourceAnswers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R129 and R130 — the envelope every route answers in.
 *
 * <p>The second test is a census rather than a behaviour: the source installs one exception
 * handler around every route and this target has no equivalent hook, so the guard is written at
 * each route by hand. A rule written forty-one times is a rule that can be missing from the
 * forty-second, and a caller cannot tell which route it is talking to from the shape of the
 * failure — so the check is that every routed method goes through it, not that one does.
 */
class ResponsesTest {

  /** The response's own bytes, which is the only place its shape is visible. */
  private static String body(HttpResponse response) {
    return ((akka.http.javadsl.model.HttpEntity.Strict) response.entity())
        .getData()
        .decodeString(StandardCharsets.UTF_8);
  }

  private static final Pattern ROUTE =
      Pattern.compile("@(?:Get|Post|Put|Patch|Delete)\\(\"[^\"]*\"\\)\\s*\\n"
          + "\\s*public HttpResponse (\\w+)\\(([^)]*)\\) \\{\\n(\\s*)(.*)");

  /** R130: an unplanned failure becomes the source's own body, and is not re-raised. */
  @Test
  void anUnplannedFailureAnswersTheSourcesShape() {
    JsonNode recorded = SourceAnswers.LIVE_SERVER.get("policy_unknown_base");
    HttpResponse response =
        Responses.guarded(
            () -> {
              throw new IllegalStateException("something nobody planned for");
            });
    assertEquals(recorded.get("status").asInt(), response.status().intValue());
    assertEquals(recorded.get("body").toString(), body(response));
  }

  /** A route that answers normally is handed straight back. */
  @Test
  void aNormalAnswerIsNotTouched() {
    HttpResponse ok = Responses.statusOk();
    assertEquals(ok.status(), Responses.guarded(() -> ok).status());
  }

  /** R129: the four headers, on the failure as well as on the success. */
  @Test
  void everyAnswerCarriesTheCorsHeaders() {
    for (HttpResponse response :
        List.of(
            Responses.statusOk(),
            Responses.notFound(),
            Responses.uncaught(),
            Responses.noContent(),
            Responses.created(),
            Responses.redirect("http://x"),
            Responses.html("<html></html>"))) {
      List<String> names = new ArrayList<>();
      response
          .getHeaders()
          .forEach(header -> names.add(header.name().toLowerCase(java.util.Locale.ROOT)));
      assertTrue(names.contains("access-control-allow-origin"), names.toString());
      assertTrue(names.contains("access-control-allow-credentials"), names.toString());
      assertTrue(names.contains("access-control-allow-methods"), names.toString());
      assertTrue(names.contains("access-control-allow-headers"), names.toString());
    }
  }

  /** R130: every routed method, without exception, answers through the guard. */
  @Test
  void everyRouteGoesThroughTheGuard() throws IOException {
    Path api = Path.of("src", "main", "java", "io", "akka", "opal");
    List<String> unguarded = new ArrayList<>();
    int routes = 0;

    try (Stream<Path> files = Files.walk(api)) {
      for (Path file : files.filter(p -> p.getFileName().toString().endsWith("Endpoint.java"))
          .toList()) {
        String text = Files.readString(file, StandardCharsets.UTF_8);
        Matcher matcher = ROUTE.matcher(text);
        while (matcher.find()) {
          routes++;
          if (!matcher.group(4).startsWith("return Responses.guarded(")) {
            unguarded.add(file.getFileName() + "#" + matcher.group(1));
          }
        }
      }
    }

    assertTrue(routes >= 41, "the census found " + routes + " routes, which is fewer than exist");
    assertEquals(List.of(), unguarded, "routes whose first act is not the guard");
  }

  /** The 401 envelope, which is the one shape a client is documented to branch on. */
  @Test
  void anUnauthorizedAnswerCarriesTheChallenge() {
    HttpResponse response =
        Responses.unauthorized(
            new io.akka.opal.common.auth.Unauthorized("Access token is expired"));
    assertEquals(401, response.status().intValue());
    assertTrue(response.getHeader("WWW-Authenticate").isPresent());
    assertEquals("Bearer", response.getHeader("WWW-Authenticate").get().value());
    assertEquals(
        "{\"detail\":{\"error\":\"Access token is expired\"}}", body(response));
    assertTrue(
        SourceAnswers.LIVE_SECURE.toString().contains("access token was not provided"),
        "the source answers refusals in the same envelope");
  }
}
