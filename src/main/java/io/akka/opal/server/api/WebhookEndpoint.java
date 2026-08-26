package io.akka.opal.server.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCode;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.http.AbstractHttpEndpoint;
import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.config.Enums.PolicySourceTypes;
import io.akka.opal.common.config.Options.GitWebhookRequestParams;
import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.pubsub.Rpc;
import io.akka.opal.server.webhook.Webhooks;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code POST /webhook} — SPEC-002 R44 and R120 to R126.
 *
 * <p>The route publishes on an internal topic rather than pulling: the worker serving the request
 * is not necessarily the one that owns the clone, and having each of them pull would mean one
 * fetch per replica for one push.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class WebhookEndpoint extends AbstractHttpEndpoint {

  private static final Logger log = LoggerFactory.getLogger(WebhookEndpoint.class);

  private final ServerRuntime runtime;

  public WebhookEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  /**
   * The body arrives as the request entity rather than as a parsed object, because the signature
   * is an HMAC of the bytes the sender wrote. A payload parsed and re-serialised is a different
   * sequence of bytes — different whitespace, different key order — and its signature would never
   * match the one the sender computed.
   */
  @Post("/webhook")
  public HttpResponse triggerWebhook(akka.http.scaladsl.model.HttpEntity.Strict entity) {
    return Responses.guarded(requestContext(), () -> {
      byte[] rawBody = entity == null ? new byte[0] : entity.getData().toArray();
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      GitWebhookRequestParams params = runtime.config().get("POLICY_REPO_WEBHOOK_PARAMS");
      PolicySourceTypes sourceType = runtime.config().get("POLICY_SOURCE_TYPE");

      // R126: an API bundle source takes a JWT rather than a git secret, and triggers regardless.
      if (sourceType == PolicySourceTypes.Api) {
        try {
          Authn.requireLoggedIn(runtime.signer(), requestContext());
        } catch (Unauthorized e) {
          return Responses.unauthorized(e);
        }
        log.info("Triggered webhook to check API bundle URL");
        runtime.publish(ServerRuntime.POLICY_REPO_WEBHOOK_TOPIC);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("event", "webhook_trigger");
        body.put("repo_url", runtime.config().getString("POLICY_BUNDLE_URL"));
        return Responses.ok(body);
      }

      try {
        Webhooks.validateGitSecret(
            runtime.config().getString("POLICY_REPO_WEBHOOK_SECRET"),
            params,
            requestContext()
                .requestHeader(params.secret_header_name())
                .map(header -> header.value())
                .orElse(null),
            rawBody == null ? new byte[0] : rawBody);
      } catch (Webhooks.Refused refused) {
        return Responses.detail(status(refused.status), refused.getMessage());
      }

      JsonNode payload;
      try {
        payload =
            rawBody == null || rawBody.length == 0
                ? Rpc.MAPPER.createObjectNode()
                : Rpc.MAPPER.readTree(new String(rawBody, StandardCharsets.UTF_8));
      } catch (Exception e) {
        return Responses.detail(StatusCodes.BAD_REQUEST, "repo url or full name not found in payload!");
      }

      Webhooks.GitChanges changes;
      try {
        changes = Webhooks.extractGitChanges(payload);
      } catch (Webhooks.Refused refused) {
        return Responses.detail(status(refused.status), refused.getMessage());
      }

      // R121: the event is read from a header or from a body key, and defaults to `ping`.
      String event = "ping";
      if (params.event_header_name() != null) {
        event =
            requestContext()
                .requestHeader(params.event_header_name())
                .map(header -> header.value())
                .orElse("ping");
      } else if (params.event_request_key() != null) {
        event = payload.path(params.event_request_key()).asText("ping");
      } else {
        log.error(
            "Webhook config is missing both event_request_key and event_header_name. Must have at"
                + " least one.");
      }

      String policyRepoUrl = runtime.config().getString("POLICY_REPO_URL");
      String mainBranch = runtime.config().getString("POLICY_REPO_MAIN_BRANCH");
      boolean enforceBranch =
          Boolean.TRUE.equals(runtime.config().get("POLICY_REPO_WEBHOOK_ENFORCE_BRANCH"));
      Webhooks.Decision decision =
          Webhooks.decide(changes, event, policyRepoUrl, params, enforceBranch, mainBranch);

      if (decision.ignoredBranch()) {
        log.warn(
            "Git Webhook ignored - POLICY_REPO_WEBHOOK_ENFORCE_BRANCH is enabled, and"
                + " POLICY_REPO_MAIN_BRANCH is `{}` but received webhook for a different branch ({})",
            mainBranch,
            changes.branch());
        return Responses.json(StatusCodes.OK, null);
      }
      if (decision.trigger()) {
        runtime.publish(ServerRuntime.POLICY_REPO_WEBHOOK_TOPIC);
      }
      Map<String, Object> body = new LinkedHashMap<>();
      body.put("status", decision.status());
      body.put("event", decision.event());
      if (decision.repoUrl() != null) {
        log.info("triggered webhook on repo: {}", decision.repoUrl());
        body.put("repo_url", decision.repoUrl());
      } else {
        log.warn(
            "Got an unexpected webhook not matching the tracked repo ({}) - with these URLS: {} and"
                + " those names: {}.",
            policyRepoUrl,
            changes.urls(),
            changes.names());
      }
      return Responses.ok(body);
    });
  }

  private static StatusCode status(int code) {
    return StatusCodes.get(code);
  }
}
