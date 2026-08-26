package io.akka.opal.server.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Delete;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.annotations.http.Put;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Authz;
import io.akka.opal.common.auth.Keys;
import io.akka.opal.common.auth.Types.EncryptionKeyFormat;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.PolicySource;
import io.akka.opal.common.schemas.Scopes;
import io.akka.opal.common.schemas.Security.PeerType;
import io.akka.opal.common.topics.Topics;
import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.scopes.GitPolicyFetcher;
import io.akka.opal.server.scopes.ScopeRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The nine multi-tenant routes — SPEC-002 R102 to R111.
 *
 * <p>They are mounted only with {@code SCOPES} on, and answer 404 otherwise, because a
 * single-tenant deployment that stumbled onto them would otherwise be told about a store it does
 * not have.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/scopes")
public class ScopesEndpoint extends AbstractHttpEndpoint {

  private static final Logger log = LoggerFactory.getLogger(ScopesEndpoint.class);

  private final ServerRuntime runtime;

  public ScopesEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  private boolean mounted() {
    return Role.isServer() && runtime.scopesEnabled();
  }

  private Map<String, Object> requireDatasource() {
    Map<String, Object> claims = Authn.requireLoggedIn(runtime.signer(), requestContext());
    Authz.requirePeerType(runtime.signer().enabled(), claims, PeerType.datasource);
    return claims;
  }

  /**
   * R110: the bundle and data routes additionally check the token's allowed scopes.
   *
   * <p>A token carrying no such claim is refused, not admitted. The claim is the whole of the
   * multi-tenant boundary on these two routes — a bundle is one tenant's policy — and a missing
   * claim read as "no restriction" would hand every tenant's bundle to any token the server
   * signed.
   */
  private void requireAllowedScope(String scopeId) {
    if (!runtime.signer().enabled()) {
      return;
    }
    Map<String, Object> claims = Authn.requireLoggedIn(runtime.signer(), requestContext());
    List<String> allowed = Authz.asStrings(claims.get("allowed_scopes"));
    if (allowed.isEmpty() || !allowed.contains(scopeId)) {
      throw new Forbidden();
    }
  }

  /** Raised when a token is valid but does not name this scope. */
  private static final class Forbidden extends RuntimeException {}

  /** R103: validate an SSH key before storing anything, and purge a repointed source. */
  @Put("/")
  public HttpResponse putScope(Scopes.Scope scope) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireDatasource();
      } catch (Unauthorized e) {
        log.error("Unauthorized to PUT scope: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      HttpResponse invalidKey = verifyPrivateKeyOrThrow(scope);
      if (invalidKey != null) {
        return invalidKey;
      }

      String oldSourceId =
          runtime
              .scopes()
              .find(scope.scope_id())
              .map(existing -> runtime.scopesService().sourceIdOf(existing.policy()))
              .orElse(null);
      String newSourceId = runtime.scopesService().sourceIdOf(scope.policy());

      runtime.scopes().put(scope);
      if (oldSourceId != null && !oldSourceId.equals(newSourceId)) {
        runtime.scopesService().publishPurge(oldSourceId, scope.scope_id());
      }
      boolean forceFetch =
          requestContext().queryParams().getBoolean("force_fetch").orElse(false);
      runtime.scopesService().refreshScope(scope, forceFetch ? null : "");
      return Responses.created();
    });
  }

  /** R103: a key with no newline, or one that parses as neither PEM nor SSH, is a 422. */
  static HttpResponse verifyPrivateKeyOrThrow(Scopes.Scope scope) {
    if (!(scope.policy().auth() instanceof PolicySource.SSHAuthData ssh)) {
      return null;
    }
    if (ssh.private_key() == null || !ssh.private_key().contains("\n")) {
      return Responses.detail(
          StatusCodes.UNPROCESSABLE_CONTENT,
          Map.of("error", "private key is expected to contain newlines!"));
    }
    boolean readable =
        parses(ssh.private_key(), EncryptionKeyFormat.pem)
            || parses(ssh.private_key(), EncryptionKeyFormat.ssh);
    if (!readable) {
      return Responses.detail(
          StatusCodes.UNPROCESSABLE_CONTENT, Map.of("error", "private key is invalid"));
    }
    return null;
  }

  private static boolean parses(String privateKey, EncryptionKeyFormat format) {
    try {
      Keys.parsePrivate(
          privateKey, format, null, io.akka.opal.common.auth.Types.JWTAlgorithm.RS256);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /** R104: the auth object is never returned. */
  @Get("/")
  public HttpResponse getAllScopes() {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireDatasource();
      } catch (Unauthorized e) {
        log.error("Unauthorized to get scopes: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      List<Scopes.RedactedScope> redacted = new ArrayList<>();
      for (Scopes.Scope scope : runtime.scopes().all()) {
        redacted.add(Scopes.redact(scope));
      }
      return Responses.ok(redacted);
    });
  }

  @Get("/{scopeId}")
  public HttpResponse getScope(String scopeId) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireDatasource();
      } catch (Unauthorized e) {
        log.error("Unauthorized to get scope: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      try {
        return Responses.ok(Scopes.redact(runtime.scopes().get(scopeId)));
      } catch (ScopeRepository.ScopeNotFound e) {
        return Responses.detail(StatusCodes.NOT_FOUND, "No such scope: " + scopeId);
      }
    });
  }

  /** R105: 204 whether or not the scope existed. */
  @Delete("/{scopeId}")
  public HttpResponse deleteScope(String scopeId) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireDatasource();
      } catch (Unauthorized e) {
        log.error("Unauthorized to delete scope: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      runtime.scopesService().deleteScope(scopeId);
      return Responses.noContent();
    });
  }

  /** R106: with no hinted hash, force a remote fetch. */
  @Post("/{scopeId}/refresh")
  public HttpResponse refreshScope(String scopeId) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireDatasource();
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      String hintedHash = requestContext().queryParams().getString("hinted_hash").orElse(null);
      try {
        Scopes.Scope scope = runtime.scopes().get(scopeId);
        runtime.scopesService().refreshScope(scope, hintedHash);
        return Responses.json(StatusCodes.OK, null);
      } catch (ScopeRepository.ScopeNotFound e) {
        return Responses.detail(StatusCodes.NOT_FOUND, "No such scope: " + scopeId);
      }
    });
  }

  @Post("/refresh")
  public HttpResponse syncAllScopes() {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireDatasource();
      } catch (Unauthorized e) {
        log.error("Unauthorized to refresh all scopes: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      runtime.publish(ServerRuntime.POLICY_REPO_WEBHOOK_TOPIC);
      return Responses.json(StatusCodes.OK, null);
    });
  }

  /** R107: an unknown scope is served the default scope's bundle, and 404 only without one. */
  @Get("/{scopeId}/policy")
  public HttpResponse getScopePolicy(String scopeId) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireAllowedScope(scopeId);
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      } catch (Forbidden e) {
        return Responses.detail(StatusCodes.FORBIDDEN, "Forbidden");
      }
      String baseHash = requestContext().queryParams().getString("base_hash").orElse(null);
      Scopes.Scope scope;
      try {
        scope = runtime.scopes().get(scopeId);
      } catch (ScopeRepository.ScopeNotFound e) {
        log.warn("Requested scope {} not found, returning default scope", scopeId);
        runtime
            .metrics()
            .event(
                "ScopeNotFound",
                "Scope " + scopeId + " not found. Serving default scope instead",
                Map.of("scope_id", scopeId));
        try {
          scope = runtime.scopes().get("default");
          baseHash = null;
        } catch (ScopeRepository.ScopeNotFound missing) {
          return Responses.detail(StatusCodes.NOT_FOUND, "No such scope: " + scopeId);
        }
      }
      try {
        GitPolicyFetcher fetcher = runtime.scopesService().fetcherFor(scope);
        return Responses.ok(fetcher.makeBundle(baseHash));
      } catch (GitPolicyFetcher.CloneNotPopulated first) {
        // R242: the clone is on its way. Hold the request rather than refusing it — a client
        // makes a handful of attempts over half a minute and then goes quiet until something
        // wakes it, so a clone that outlives those attempts leaves that client with no policy
        // at all. Waiting turns a gap into latency the client already tolerates.
        Boolean waited = waitForClone(scope);
        if (waited == null) {
          runtime
              .metrics()
              .event(
                  "ScopePolicyUnavailable",
                  "Scope " + scopeId + " policy 503 (too many waiting)",
                  Map.of("scope_id", scopeId, "status", "503", "retryable", "true"));
          return retryAfter(
              StatusCodes.SERVICE_UNAVAILABLE,
              "Policy clone for scope " + scopeId + " is being created, retry shortly",
              "30");
        }
        if (waited) {
          try {
            return Responses.ok(runtime.scopesService().fetcherFor(scope).makeBundle(baseHash));
          } catch (RuntimeException stillNot) {
            log.warn("Scope {} clone was ready and the bundle still failed", scopeId, stillNot);
          }
        }
        runtime
            .metrics()
            .event(
                "ScopePolicyUnavailable",
                "Scope " + scopeId + " policy 503 (clone in progress)",
                Map.of("scope_id", scopeId, "status", "503", "retryable", "true"));
        return retryAfter(
            StatusCodes.SERVICE_UNAVAILABLE,
            "Policy clone for scope " + scopeId + " is being created, retry shortly",
            "30");
      } catch (GitPolicyFetcher.BranchHeadNotFound e) {
        // R243: the clone is there and the branch this scope names is not, which no amount of
        // waiting fixes. A caller told to retry would retry until somebody edited the scope.
        runtime
            .metrics()
            .event(
                "ScopePolicyUnavailable",
                "Scope " + scopeId + " policy 409 (branch unresolved)",
                Map.of("scope_id", scopeId, "status", "409", "retryable", "false"));
        return Responses.detail(
            StatusCodes.CONFLICT,
            "Policy branch for scope " + scopeId + " could not be resolved");
      } catch (Exception e) {
        log.warn("Scope {} is live but its clone is unavailable ({}), returning 503", scopeId, e);
        runtime
            .metrics()
            .event(
                "ScopePolicyUnavailable",
                "Scope " + scopeId + " policy 503 (clone unavailable)",
                Map.of("scope_id", scopeId, "status", "503", "retryable", "true"));
        return retryAfter(
            StatusCodes.SERVICE_UNAVAILABLE,
            "Policy clone for scope " + scopeId + " is temporarily unavailable, retry shortly",
            "5");
      }
    });
  }

  /** R242: how many requests this process is holding inside the wait, and the ceiling on it. */
  private static final java.util.concurrent.atomic.AtomicInteger CLONE_WAIT_IN_FLIGHT =
      new java.util.concurrent.atomic.AtomicInteger();

  /** The longest hold, whatever the configuration says: past this a load balancer gives up first. */
  static final double CLONE_WAIT_CEILING_SECONDS = 55;

  private static final String CLONE_WAIT_METRIC = "opal_server.scopes.policy_clone_wait";

  /**
   * The two names written out rather than composed.
   *
   * <p>A name built by appending a suffix to another does not appear in this file, and a census
   * that looks for what the original measures cannot find it.
   */
  private static final String CLONE_WAIT_INFLIGHT_METRIC =
      "opal_server.scopes.policy_clone_wait_inflight";

  private static final String CLONE_WAIT_SECONDS_METRIC =
      "opal_server.scopes.policy_clone_wait_seconds";

  /**
   * R242: holds the request while the clone is being populated.
   *
   * <p>Answers true when the clone became usable, false when the wait ran out, and null when this
   * process is already holding as many requests as it is allowed to — which is refused
   * immediately, because releasing a thousand held requests at once builds a thousand bundles at
   * once and every one of them then misses the deadline the wait existed to protect.
   */
  private Boolean waitForClone(Scopes.Scope scope) {
    double budget = readDouble("SCOPES_POLICY_CLONE_WAIT_SECONDS");
    if (!Double.isFinite(budget) || budget <= 0) {
      return false;
    }
    budget = Math.min(budget, CLONE_WAIT_CEILING_SECONDS);
    int ceiling = (Integer) runtime.config().get("SCOPES_POLICY_CLONE_WAIT_MAX_INFLIGHT");
    int inFlight = CLONE_WAIT_IN_FLIGHT.incrementAndGet();
    try {
      runtime
          .metrics()
          .gauge(
              CLONE_WAIT_INFLIGHT_METRIC,
              (long) inFlight,
              Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
      if (ceiling > 0 && inFlight > ceiling) {
        runtime.metrics().increment(CLONE_WAIT_METRIC, Map.of("outcome", "shed"));
        return null;
      }
      long started = System.nanoTime();
      long deadline = started + (long) (budget * 1_000_000_000L);
      while (System.nanoTime() < deadline) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
        if (runtime.scopesService().cloneReady(scope)) {
          record("served", started);
          return true;
        }
      }
      record("timeout", started);
      return false;
    } finally {
      int remaining = CLONE_WAIT_IN_FLIGHT.decrementAndGet();
      runtime
          .metrics()
          .gauge(
              CLONE_WAIT_INFLIGHT_METRIC,
              (long) remaining,
              Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
    }
  }

  private void record(String outcome, long startedNanos) {
    runtime.metrics().increment(CLONE_WAIT_METRIC, Map.of("outcome", outcome));
    runtime
        .metrics()
        .gauge(
            CLONE_WAIT_SECONDS_METRIC,
            (System.nanoTime() - startedNanos) / 1_000_000_000.0,
            Map.of("outcome", outcome));
  }

  private double readDouble(String entry) {
    try {
      return Double.parseDouble(runtime.config().getString(entry));
    } catch (RuntimeException e) {
      return 0;
    }
  }

  /** R244: a refusal a caller can act on says how long to wait before asking again. */
  private static akka.http.javadsl.model.HttpResponse retryAfter(
      akka.http.javadsl.model.StatusCode status, String detail, String seconds) {
    return Responses.detail(status, detail)
        .addHeader(akka.http.javadsl.model.headers.RawHeader.create("Retry-After", seconds));
  }

  /** R108: the scope's own data configuration, falling back to the server's. */
  @Get("/{scopeId}/data")
  public HttpResponse getScopeDataConfig(String scopeId) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        requireAllowedScope(scopeId);
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      } catch (Forbidden e) {
        return Responses.detail(StatusCodes.FORBIDDEN, "Forbidden");
      }
      log.info("Serving source configuration for scope {}", scopeId);
      try {
        return Responses.ok(runtime.scopes().get(scopeId).data());
      } catch (ScopeRepository.ScopeNotFound e) {
        log.warn("Requested scope {} not found, returning OPAL_DATA_CONFIG_SOURCES", scopeId);
        Data.ServerDataSourceConfig config = runtime.config().get("DATA_CONFIG_SOURCES");
        if (config == null) {
          return Responses.detail(StatusCodes.NOT_FOUND, "No such scope: " + scopeId);
        }
        if (config.external_source_url() != null) {
          return Responses.redirect(
              DataEndpoint.withQueryParam(
                  config.external_source_url(), "token", Authn.bearerToken(requestContext())));
        }
        return Responses.ok(config.config());
      }
    });
  }

  /** R109: every topic is prefixed with {@code data:} and published under the scope. */
  @Post("/{scopeId}/data/update")
  public HttpResponse publishDataUpdateEvent(String scopeId, Data.DataUpdate update) {
    return Responses.guarded(requestContext(), () -> {
      if (!mounted()) {
        return Responses.notFound();
      }
      try {
        Map<String, Object> claims = requireDatasource();
        Authz.restrictOptionalTopicsToPublish(runtime.signer().enabled(), claims, update);
      } catch (Unauthorized e) {
        log.error("Unauthorized to publish update: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      List<Data.DataSourceEntry> prefixed = new ArrayList<>();
      for (Data.DataSourceEntry entry : update.entries()) {
        List<String> topics = new ArrayList<>();
        for (String topic : entry.topics()) {
          topics.add("data:" + topic);
        }
        prefixed.add(entry.withTopics(topics));
      }
      runtime.publishDataUpdate(
          new Data.DataUpdate(update.id(), prefixed, update.reason(), update.callback()), scopeId);
      return Responses.json(StatusCodes.OK, null);
    });
  }

  /** Kept so a reader can see the expansion the scoped publisher relies on. */
  static List<String> expand(List<String> topics) {
    return new ArrayList<>(Topics.expandAll(topics));
  }
}
