package io.akka.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R129 — the headers a browser needs, on every answer this port produces.
 *
 * <p>The half that matters is the error path. A cross-origin call that gets its headers on a 200
 * and not on a 401 shows in the browser as a CORS failure rather than as the 401 it was, which
 * makes a caller's own authentication bug unreadable.
 *
 * <p>Two things here are measured rather than asserted equal to the original, and both are in the
 * README's divergence list. The runtime adds a development-mode {@code *} of its own alongside
 * the configured origin, so the check is that the four configured headers are present rather than
 * that they are the only ones. And the preflight is written onto the socket by hand, because the
 * SDK's route annotations cover five verbs and {@code OPTIONS} is not one of them — what the
 * runtime does with it is recorded here.
 */
public class CorsIntegrationTest extends TestKitSupport {

  static {
    System.setProperty("OPAL_ROLE", "server");
    System.setProperty("OPAL_ALLOWED_ORIGINS", "https://console.example.com");
  }

  @AfterAll
  public static void clearProperties() {
    System.clearProperty("OPAL_ROLE");
    System.clearProperty("OPAL_ALLOWED_ORIGINS");
  }

  private static final List<String> EXPECTED =
      List.of(
          "access-control-allow-credentials: true",
          "access-control-allow-headers: *",
          "access-control-allow-methods: *",
          "access-control-allow-origin: https://console.example.com");

  private static List<String> corsHeaders(StrictResponse<ByteString> response) {
    List<String> found = new ArrayList<>();
    response
        .httpResponse()
        .getHeaders()
        .forEach(
            header -> {
              if (header.name().toLowerCase(Locale.ROOT).startsWith("access-control-")) {
                found.add(header.name().toLowerCase(Locale.ROOT) + ": " + header.value());
              }
            });
    java.util.Collections.sort(found);
    return found;
  }

  /** R129: the configured origin, credentials, every method and every header. */
  @Test
  public void anOrdinaryAnswerCarriesTheConfiguredOrigin() {
    StrictResponse<ByteString> response = httpClient.GET("/").invoke();
    assertEquals(200, response.status().intValue());
    assertTrue(corsHeaders(response).containsAll(EXPECTED), corsHeaders(response).toString());
  }

  /** R129: and so does a failure, which is the half a browser makes unreadable without it. */
  @Test
  public void aFailureCarriesThemToo() {
    StrictResponse<ByteString> disabled = httpClient.GET("/statistics").invoke();
    assertEquals(501, disabled.status().intValue(), "statistics are off in this run");
    assertTrue(corsHeaders(disabled).containsAll(EXPECTED), corsHeaders(disabled).toString());

    StrictResponse<ByteString> otherRole = httpClient.GET("/ready").invoke();
    assertEquals(404, otherRole.status().intValue(), "a client route, on a server");
    assertTrue(corsHeaders(otherRole).containsAll(EXPECTED), corsHeaders(otherRole).toString());
  }

  /**
   * What this rebuild does with a preflight, measured. The original's middleware answers 200 with
   * the same four headers; this is recorded as a divergence rather than asserted to match.
   */
  @Test
  public void aPreflightIsAnsweredByTheRuntime() throws Exception {
    String host = testKit.getHost();
    int port = testKit.getPort();
    try (Socket socket = new Socket(host, port)) {
      OutputStream out = socket.getOutputStream();
      out.write(
          ("OPTIONS / HTTP/1.1\r\n"
                  + "Host: "
                  + host
                  + "\r\n"
                  + "Origin: https://console.example.com\r\n"
                  + "Access-Control-Request-Method: GET\r\n"
                  + "Connection: close\r\n\r\n")
              .getBytes(StandardCharsets.US_ASCII));
      out.flush();
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
      String statusLine = reader.readLine();
      assertTrue(
          statusLine != null && statusLine.startsWith("HTTP/1.1 "), String.valueOf(statusLine));
      assertEquals(
          204,
          Integer.parseInt(statusLine.split(" ")[1]),
          "the runtime answers the preflight itself, with no body");
    }
  }
}
