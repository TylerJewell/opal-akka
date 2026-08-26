package io.akka.opal.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** A scope: an id, a policy source, and a base data configuration — SPEC-002 section 2.3. */
public final class Scopes {

  private Scopes() {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Scope(
      String scope_id, PolicySource.GitPolicyScopeSource policy, Data.DataSourceConfig data) {
    public Scope {
      if (data == null) {
        data = new Data.DataSourceConfig(List.of());
      }
    }
  }

  /**
   * A scope as the read routes return it — R104.
   *
   * <p>A separate type rather than the same one with a null field: the source's auth is a
   * discriminated union whose variants default their own discriminator, so a null there comes
   * back out as {@code {"auth_type": "none"}} — which is not an omission, it is a claim that the
   * scope has no credentials. The field is absent here because absent is what R104 says.
   */
  public record RedactedSource(
      String source_type,
      String url,
      List<String> directories,
      List<String> extensions,
      List<String> bundle_ignore,
      String manifest,
      Boolean poll_updates,
      String branch) {}

  public record RedactedScope(
      String scope_id, RedactedSource policy, Data.DataSourceConfig data) {}

  public static RedactedScope redact(Scope scope) {
    PolicySource.GitPolicyScopeSource policy = scope.policy();
    return new RedactedScope(
        scope.scope_id(),
        new RedactedSource(
            policy.source_type(),
            policy.url(),
            policy.directories(),
            policy.extensions(),
            policy.bundle_ignore(),
            policy.manifest(),
            policy.poll_updates(),
            policy.branch()),
        scope.data());
  }
}
