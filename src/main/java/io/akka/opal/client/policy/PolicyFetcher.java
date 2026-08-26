package io.akka.opal.client.policy;

import io.akka.opal.common.config.Options.ConnRetryOptions;
import io.akka.opal.common.schemas.Policy;
import io.akka.opal.common.util.Http;
import io.akka.opal.server.pubsub.Rpc;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetching a policy bundle from the server — SPEC-002 R93.
 *
 * <p>The endpoint is the scoped one under a non-default scope and the plain one otherwise, which
 * is the only difference a scoped client sees in this half of the protocol.
 */
public final class PolicyFetcher {

  private static final Logger log = LoggerFactory.getLogger(PolicyFetcher.class);

  private final String policyEndpointUrl;
  private final String token;
  private final ConnRetryOptions retry;
  private final HttpClient http;

  public PolicyFetcher(String backendUrl, String token, String scopeId, ConnRetryOptions retry) {
    this.token = token;
    this.retry = retry == null ? ConnRetryOptions.defaults() : retry;
    this.http = Http.forClient();
    this.policyEndpointUrl =
        scopeId != null && !scopeId.equals("default")
            ? backendUrl + "/scopes/" + scopeId + "/policy"
            : backendUrl + "/policy";
  }

  public String policyEndpointUrl() {
    return policyEndpointUrl;
  }

  public Policy.PolicyBundle fetchPolicyBundle(List<String> directories, String baseHash) {
    Exception last = null;
    for (int attempt = 1; attempt <= retry.attempts(); attempt++) {
      try {
        return fetchOnce(directories, baseHash);
      } catch (Exception e) {
        last = e;
        try {
          Thread.sleep(
              retry.waitMillis(attempt, java.util.concurrent.ThreadLocalRandom.current()));
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    log.warn("Failed all attempts to fetch bundle, got error: {}", last);
    throw new IllegalStateException("could not fetch a policy bundle", last);
  }

  private Policy.PolicyBundle fetchOnce(List<String> directories, String baseHash)
      throws Exception {
    StringJoiner query = new StringJoiner("&");
    for (String directory : directories) {
      query.add("path=" + URLEncoder.encode(directory, StandardCharsets.UTF_8));
    }
    if (baseHash != null) {
      query.add("base_hash=" + URLEncoder.encode(baseHash, StandardCharsets.UTF_8));
    }
    String url = policyEndpointUrl + (query.length() == 0 ? "" : "?" + query);
    log.info("Fetching policy bundle from {}", policyEndpointUrl);

    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(60))
            .header("content-type", "text/plain")
            .GET();
    if (token != null) {
      request.header("Authorization", "Bearer " + token);
    }
    HttpResponse<String> response =
        http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (response.statusCode() == 404) {
      log.warn("requested paths not found: {}", directories);
      throw new IllegalStateException(
          "requested path " + policyEndpointUrl + " was not found in the policy repo!");
    }
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "unexpected response code while fetching bundle: " + response.statusCode());
    }
    Policy.PolicyBundle bundle =
        Rpc.MAPPER.readValue(response.body(), Policy.PolicyBundle.class);
    log.info("Fetched valid bundle, id: {}", bundle.hash());
    return bundle;
  }
}
