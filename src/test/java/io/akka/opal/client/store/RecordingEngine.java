package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A policy engine that answers OPA's routes and remembers, in order, what was asked of it.
 *
 * <p>The engine is what the source's own probe stood in for when it recorded the call order these
 * tests compare against: it replaced the four write methods on a real client and kept the names
 * they were called with. Standing in for the engine over real HTTP keeps the request building,
 * the accepted-status lists and the retry loop — everything the rule is actually about — inside
 * the test, and replaces only the thing on the far side of the socket.
 */
public final class RecordingEngine implements AutoCloseable {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpServer server;
  private final List<List<String>> calls = new ArrayList<>();
  private final Map<String, String> policies = new LinkedHashMap<>();

  /** Status codes to answer with, keyed by "METHOD /path"; anything absent gets the default. */
  private final Map<String, Integer> overrides = new LinkedHashMap<>();

  public RecordingEngine(Map<String, String> initialPolicies) throws IOException {
    policies.putAll(initialPolicies);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.setExecutor(null);
    server.start();
  }

  public String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  /** Every write, in the order it arrived, as {@code [name, argument]}. */
  public List<List<String>> calls() {
    return calls;
  }

  public Map<String, String> policies() {
    return policies;
  }

  public void answerWith(String methodAndPath, int status) {
    overrides.put(methodAndPath, status);
  }

  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = URLDecoder.decode(exchange.getRequestURI().getPath(), StandardCharsets.UTF_8);
    byte[] body = exchange.getRequestBody().readAllBytes();

    int status;
    byte[] response = new byte[0];
    Integer override = overrides.get(method + " " + path);

    if (path.equals("/v1/policies") && method.equals("GET")) {
      var out = MAPPER.createObjectNode();
      var array = out.putArray("result");
      policies.forEach((id, raw) -> array.addObject().put("id", id).put("raw", raw));
      response = MAPPER.writeValueAsBytes(out);
      status = 200;
    } else if (path.startsWith("/v1/policies/")) {
      String id = path.substring("/v1/policies/".length());
      if (method.equals("PUT")) {
        record("set_policy", id);
        status = override == null ? 200 : override;
        if (status == 200) {
          policies.put(id, new String(body, StandardCharsets.UTF_8));
        }
      } else if (method.equals("DELETE")) {
        record("delete_policy", id);
        policies.remove(id);
        status = override == null ? 200 : override;
      } else {
        var out = MAPPER.createObjectNode();
        out.putObject("result").put("id", id).put("raw", policies.getOrDefault(id, ""));
        response = MAPPER.writeValueAsBytes(out);
        status = 200;
      }
    } else if (path.startsWith("/v1/data")) {
      String dataPath = path.substring("/v1/data".length());
      if (method.equals("PUT")) {
        record("set_policy_data", dataPath);
        status = override == null ? 204 : override;
      } else if (method.equals("PATCH")) {
        record("patch_policy_data", dataPath);
        status = override == null ? 204 : override;
      } else if (method.equals("DELETE")) {
        record("delete_policy_data", dataPath);
        status = override == null ? 204 : override;
      } else {
        response = "{\"result\":{}}".getBytes(StandardCharsets.UTF_8);
        status = 200;
      }
    } else if (path.equals("/health")) {
      status = 200;
    } else {
      status = 404;
    }

    exchange.getResponseHeaders().add("content-type", "application/json");
    if (status == 204 || status == 304) {
      exchange.sendResponseHeaders(status, -1);
    } else {
      exchange.sendResponseHeaders(status, response.length);
      if (response.length > 0) {
        exchange.getResponseBody().write(response);
      }
    }
    exchange.close();
  }

  private void record(String name, String argument) {
    calls.add(List.of(name, argument));
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
