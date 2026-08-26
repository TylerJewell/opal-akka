package io.akka.opal.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.akka.opal.server.pubsub.Rpc;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R136 — {@code obtain-token}, against a server that records what it was sent.
 *
 * <p>What goes out matters as much as what comes back. The command posts an access-token request
 * with the master token as a bearer credential, and a caller running it in a script reads one
 * line of output: the token and nothing else, unless it asked for the whole object.
 */
class ObtainTokenTest {

  private record Ran(int code, String output, JsonNode request, String authorization) {}

  private static Ran run(List<String> arguments, String responseBody) throws Exception {
    AtomicReference<String> body = new AtomicReference<>();
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/token",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("content-type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.start();
    try {
      ByteArrayOutputStream captured = new ByteArrayOutputStream();
      java.util.List<String> full = new java.util.ArrayList<>();
      full.add("obtain-token");
      full.addAll(arguments);
      full.add("--server-url");
      full.add("http://127.0.0.1:" + server.getAddress().getPort());
      int code =
          new OpalCli(
                  OpalCli.Which.server,
                  java.util.Map.of(),
                  new PrintStream(captured, true, StandardCharsets.UTF_8))
              .run(full.toArray(new String[0]));
      return new Ran(
          code,
          captured.toString(StandardCharsets.UTF_8),
          Rpc.MAPPER.readTree(body.get()),
          authorization.get());
    } finally {
      server.stop(0);
    }
  }

  /** R136: the master token is the bearer credential, and only the token is printed. */
  @Test
  void theMasterTokenIsSentAndOnlyTheTokenIsPrinted() throws Exception {
    Ran ran = run(List.of("MASTER", "--type", "datasource"), "{\"token\": \"abc.def.ghi\"}");
    assertEquals(0, ran.code());
    assertEquals("bearer MASTER", ran.authorization());
    assertEquals("datasource", ran.request().get("type").asText());
    assertEquals("abc.def.ghi", ran.output().trim());
  }

  /** R136: asked for the whole object, it prints the whole object. */
  @Test
  void theWholeObjectIsPrintedWhenAskedFor() throws Exception {
    Ran ran =
        run(
            List.of("MASTER", "--no-just-the-token"),
            "{\"token\": \"abc\", \"token_type\": \"bearer\"}");
    assertTrue(ran.output().contains("token_type"), ran.output());
  }

  /** R136: the time to live is a number and a unit, and the default is a year. */
  @Test
  void theTimeToLiveIsANumberAndAUnit() throws Exception {
    assertEquals(Duration.ofDays(365), OpalCli.readTtl(List.of("obtain-token", "MASTER")));
    assertEquals(Duration.ofDays(7), OpalCli.readTtl(List.of("--ttl", "7", "days")));
    assertEquals(Duration.ofHours(3), OpalCli.readTtl(List.of("--ttl", "3", "hours")));
    assertEquals(Duration.ofMinutes(5), OpalCli.readTtl(List.of("--ttl", "5", "minutes")));
    assertEquals(Duration.ofSeconds(30), OpalCli.readTtl(List.of("--ttl", "30", "seconds")));
    assertEquals(Duration.ofMillis(250), OpalCli.readTtl(List.of("--ttl", "250", "milliseconds")));

    Ran ran = run(List.of("MASTER", "--ttl", "7", "days"), "{\"token\": \"t\"}");
    assertEquals(Duration.ofDays(7).toSeconds(), ran.request().get("ttl").asLong());
  }

  /** A server that cannot be reached is a message and a non-zero exit, not a stack trace. */
  @Test
  void anUnreachableServerIsReported() {
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    int code =
        new OpalCli(
                OpalCli.Which.server,
                java.util.Map.of(),
                new PrintStream(captured, true, StandardCharsets.UTF_8))
            .run(new String[] {"obtain-token", "MASTER", "--server-url", "http://127.0.0.1:1"});
    assertEquals(1, code);
    assertTrue(captured.toString(StandardCharsets.UTF_8).startsWith("could not obtain a token: "));
  }
}
