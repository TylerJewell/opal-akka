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
import io.akka.opal.common.schemas.Policy;
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

      // R386: a previous record nobody can read must not block the write that replaces it. Its
      // source id is unknowable, so no purge can name it — but refusing the overwrite leaves the
      // tenant with the unreadable record for good.
      String oldSourceId = null;
      try {
        oldSourceId =
            runtime
                .scopes()
                .find(scope.scope_id())
                .map(existing -> runtime.scopesService().sourceIdOf(existing.policy()))
                .orElse(null);
      } catch (RuntimeException e) {
        log.warn(
            "Could not read previous record for scope {}, skipping repoint purge: {}",
            scope.scope_id(),
            e.toString());
      }
      String newSourceId = runtime.scopesService().sourceIdOf(scope.policy());

      try {
        runtime.scopes().put(scope);
      } finally {
        // R387: published even when the write's outcome is unclear. A write that committed and
        // then failed to answer leaves a retry seeing the two source ids already equal, so
        // nothing would ever name the old one again and its clone would be orphaned.
        if (oldSourceId != null && !oldSourceId.equals(newSourceId)) {
          runtime.scopesService().publishPurge(oldSourceId, scope.scope_id(), "repoint");
        }
      }
      boolean forceFetch =
          requestContext().queryParams().getBoolean("force_fetch").orElse(false);
      // R388: every replica syncs the scope, and this answers without waiting for the clone.
      // Syncing inline here makes the write's latency the clone's latency, and leaves every
      // other replica holding a stale clone until its own poll comes round.
      log.info("Sync scope: {}{}", scope.scope_id(), forceFetch ? " (force fetch)" : "");
      runtime.publish(
          ServerRuntime.POLICY_REPO_WEBHOOK_TOPIC,
          Map.of("scope_id", scope.scope_id(), "force_fetch", forceFetch));
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
        runtime.scopes().get(scopeId);
        log.info("Refresh scope: {}", scopeId);
        // R388: with no hinted hash there is no way to know whether the remote moved, so the
        // sync is told to fetch; with one, a replica that already holds that commit can skip it.
        Map<String, Object> message = new java.util.LinkedHashMap<>();
        message.put("scope_id", scopeId);
        message.put("force_fetch", hintedHash == null);
        message.put("hinted_hash", hintedHash);
        runtime.publish(ServerRuntime.POLICY_REPO_WEBHOOK_TOPIC, message);
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
      // R308: the fallback to the default scope answers a different 503 from the primary path —
      // "temporarily unavailable" with Retry-After 5 rather than "being created" with 30. A
      // caller that asked for a scope which does not exist is being served somebody else's
      // bundle as a courtesy, and is told to come back sooner.
      boolean servedByDefault = false;
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
          servedByDefault = true;
        } catch (ScopeRepository.ScopeNotFound missing) {
          return Responses.detail(StatusCodes.NOT_FOUND, "No such scope: " + scopeId);
        }
      }
      try {
        return Responses.ok(bundleWaitingForClone(scope, scopeId, baseHash));
      } catch (GitPolicyFetcher.CloneNotPopulated stillCloning) {
        // R242: the clone was still not usable when the budget ran out.
        runtime
            .metrics()
            .event(
                "ScopePolicyUnavailable",
                "Scope " + scopeId + " policy 503 (clone in progress)",
                Map.of("scope_id", scopeId, "status", "503", "retryable", "true"));
        return cloneUnavailable(scopeId, servedByDefault, "being created");
      } catch (CloneWaitShed shed) {
        runtime
            .metrics()
            .event(
                "ScopePolicyUnavailable",
                "Scope " + scopeId + " policy 503 (too many waiting)",
                Map.of("scope_id", scopeId, "status", "503", "retryable", "true"));
        return cloneUnavailable(scopeId, servedByDefault, "being created");
      } catch (GitPolicyFetcher.BranchHeadNotFound e) {
        // R243: the clone is there and the branch this scope names is not, which no amount of
        // waiting fixes - including a wait that has already happened. A caller told to retry
        // would retry until somebody edited the scope.
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
        return cloneUnavailable(scopeId, true, "temporarily unavailable");
      }
    });
  }

  /** R242: this process is already holding as many requests inside the wait as it is allowed to. */
  static final class CloneWaitShed extends RuntimeException {}

  /**
   * R242 and R401: the bundle, holding the request while the clone is populated.
   *
   * <p>The build is attempted again on every poll rather than once at the end, and only a clone
   * that is still not populated keeps the wait going. Everything else a build can raise comes
   * straight out - a branch that is not there is a 409 whether it was found before the wait or
   * after it, and a gutted object store is a retryable 503 - because a wait that relabelled
   * those would answer a permanent misconfiguration with "try again".
   *
   * <p>Exactly one outcome is counted per request that reaches the wait: served, timeout, shed,
   * disconnected, cancelled or error.
   */
  private Policy.PolicyBundle bundleWaitingForClone(
      Scopes.Scope scope, String scopeId, String baseHash) {
    try {
      return runtime.scopesService().fetcherFor(scope).makeBundle(baseHash);
    } catch (GitPolicyFetcher.CloneNotPopulated first) {
      // Fall through to the wait.
    }
    double budget = boundedCloneWait();
    if (budget <= 0) {
      throw new GitPolicyFetcher.CloneNotPopulated("clone is not populated");
    }
    int ceiling = (Integer) runtime.config().get("SCOPES_POLICY_CLONE_WAIT_MAX_INFLIGHT");
    if (ceiling > 0 && CLONE_WAIT_IN_FLIGHT.get() >= ceiling) {
      log.info(
          "Scope {} clone wait is at its {}-request cap; answering 503 without waiting",
          scopeId,
          ceiling);
      runtime.metrics().increment(CLONE_WAIT_METRIC, Map.of("outcome", "shed"));
      throw new CloneWaitShed();
    }
    long started = System.nanoTime();
    long deadline = started + (long) (budget * 1_000_000_000L);
    String outcome = "error";
    int inFlight = CLONE_WAIT_IN_FLIGHT.incrementAndGet();
    try {
      publishInFlight(inFlight);
      while (true) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          outcome = "timeout";
          throw new GitPolicyFetcher.CloneNotPopulated("clone is not populated");
        }
        try {
          Thread.sleep(Math.min(CLONE_WAIT_POLL_MILLIS, remaining / 1_000_000L + 1));
        } catch (InterruptedException e) {
          // R402: a wait torn down is counted as one. Left out, a fleet whose waits are all
          // being cancelled looks exactly like a fleet where nothing is waiting.
          Thread.currentThread().interrupt();
          outcome = "cancelled";
          log.info("Scope {} clone wait cancelled after {}s", scopeId, secondsSince(started));
          throw new GitPolicyFetcher.CloneNotPopulated("clone wait cancelled");
        }
        // R403: the caller may have gone. Nobody is waiting for that bundle, and the slot is
        // worth more to a request that is still listening.
        if (callerHasGone()) {
          outcome = "disconnected";
          log.info(
              "Scope {} clone wait abandoned after {}s: the client disconnected",
              scopeId,
              secondsSince(started));
          throw new GitPolicyFetcher.CloneNotPopulated("the client disconnected");
        }
        try {
          Policy.PolicyBundle bundle =
              runtime.scopesService().fetcherFor(scope).makeBundle(baseHash);
          outcome = "served";
          log.info(
              "Scope {} clone became available after {}s wait", scopeId, secondsSince(started));
          return bundle;
        } catch (GitPolicyFetcher.CloneNotPopulated again) {
          // Still being populated; keep waiting.
        } catch (RuntimeException other) {
          log.info(
              "Scope {} held {}s waiting for its clone before failing: {}",
              scopeId,
              secondsSince(started),
              other.toString());
          throw other;
        }
      }
    } finally {
      int left = CLONE_WAIT_IN_FLIGHT.decrementAndGet();
      publishInFlight(left);
      runtime.metrics().increment(CLONE_WAIT_METRIC, Map.of("outcome", outcome));
      if (outcome.equals("served") || outcome.equals("timeout")) {
        runtime
            .metrics()
            .gauge(
                CLONE_WAIT_SECONDS_METRIC,
                Math.round(secondsSince(started)),
                Map.of("outcome", outcome));
      }
    }
  }

  private static double secondsSince(long startedNanos) {
    return Math.round((System.nanoTime() - startedNanos) / 100_000_000.0) / 10.0;
  }

  private void publishInFlight(int count) {
    runtime
        .metrics()
        .gauge(
            CLONE_WAIT_INFLIGHT_METRIC,
            (long) count,
            Map.of("pid", String.valueOf(ProcessHandle.current().pid())));
  }

  /**
   * R404: whether the caller has hung up.
   *
   * <p>The runtime's HTTP layer hands a handler a finished request and takes an answer back; it
   * does not tell the handler that the connection went away underneath it, and the handler is
   * not asynchronous, so there is nothing to observe. This answers false, and the
   * {@code disconnected} outcome is therefore never counted here - a difference from the source
   * recorded in the README rather than a gap in the accounting.
   */
  private boolean callerHasGone() {
    return false;
  }

  /** How often the wait looks again. One poll is a directory read and a ref listing. */
  static final long CLONE_WAIT_POLL_MILLIS = 1000;

  /**
   * R405: the configured hold, validated and clamped, with the clamp said once.
   *
   * <p>A hold longer than a load balancer's idle timeout is served as a gateway timeout rather
   * than as a bundle, which is the failure the wait exists to prevent. The warning latches
   * because a misconfigured fleet would otherwise emit one identical line per request.
   */
  private double boundedCloneWait() {
    double budget = readDouble("SCOPES_POLICY_CLONE_WAIT_SECONDS");
    if (!Double.isFinite(budget) || budget <= 0) {
      return 0;
    }
    if (budget > CLONE_WAIT_CEILING_SECONDS) {
      if (CLAMP_LOGGED.compareAndSet(false, true)) {
        log.warn(
            "SCOPES_POLICY_CLONE_WAIT_SECONDS={}s exceeds the {}s ceiling and is clamped: a hold"
                + " longer than the load balancer's idle timeout is served as a 504, not as a"
                + " bundle",
            budget,
            CLONE_WAIT_CEILING_SECONDS);
      }
      return CLONE_WAIT_CEILING_SECONDS;
    }
    return budget;
  }

  private static final java.util.concurrent.atomic.AtomicBoolean CLAMP_LOGGED =
      new java.util.concurrent.atomic.AtomicBoolean();

  /**
   * The 503 a clone that cannot answer yet produces.
   *
   * <p>Two shapes: the primary path says the clone is being created and asks for thirty seconds,
   * the default-scope fallback says it is temporarily unavailable and asks for five.
   */
  private HttpResponse cloneUnavailable(String scopeId, boolean servedByDefault, String reason) {
    if (servedByDefault) {
      return retryAfter(
          StatusCodes.SERVICE_UNAVAILABLE,
          "Policy clone for scope " + scopeId + " is temporarily unavailable, retry shortly",
          "5");
    }
    return retryAfter(
        StatusCodes.SERVICE_UNAVAILABLE,
        "Policy clone for scope " + scopeId + " is " + reason + ", retry shortly",
        "30");
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
