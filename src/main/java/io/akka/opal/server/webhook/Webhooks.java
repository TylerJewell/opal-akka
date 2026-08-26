package io.akka.opal.server.webhook;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.config.Options.GitWebhookRequestParams;
import io.akka.opal.common.config.Options.SecretTypeEnum;
import io.akka.opal.common.util.Aws;
import io.akka.opal.common.util.Hashing;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reading a git provider's webhook — SPEC-002 R120 to R126.
 *
 * <p>The repository a webhook names is collected from nine payload fields across four providers,
 * because each writes it somewhere different and OPAL accepts all of them. A payload from which
 * neither a URL nor an {@code owner/name} can be read is a 400: it is not a webhook this can act
 * on, and treating it as a trigger would let anything at all force a pull.
 */
public final class Webhooks {

  /** Raised when the payload or the secret is not one this may act on. */
  public static final class Refused extends RuntimeException {
    public final int status;

    public Refused(int status, String message) {
      super(message);
      this.status = status;
    }
  }

  /** What a webhook payload says has changed. */
  public record GitChanges(List<String> urls, String branch, List<String> names) {}

  /**
   * What a webhook amounts to, once the payload has been read — SPEC-002 R121, R124, R125.
   *
   * <p>{@code ignoredBranch} is not the same answer as {@code status: "ignored"}: a webhook for
   * the wrong branch answers with no body at all, and one for the wrong repository answers a body
   * saying so. A caller can tell the two apart and the difference is worth keeping.
   */
  public record Decision(String status, String event, String repoUrl, boolean trigger,
      boolean ignoredBranch) {}

  /**
   * The whole decision, as a function of the payload and the configuration.
   *
   * <p>Separate from the route so the combinations can be enumerated: three inputs each with two
   * answers is eight outcomes, and the route can only be asked about one configuration per
   * process, because a configuration is read once at start-up.
   */
  public static Decision decide(
      GitChanges changes,
      String event,
      String policyRepoUrl,
      GitWebhookRequestParams params,
      boolean enforceBranch,
      String mainBranch) {
    if (enforceBranch && !java.util.Objects.equals(mainBranch, changes.branch())) {
      return new Decision(null, event, null, false, true);
    }
    boolean matches =
        policyRepoUrl != null
            && !policyRepoUrl.isEmpty()
            && (isMatchingWebhookUrl(policyRepoUrl, changes.urls(), changes.names())
                || !Boolean.TRUE.equals(params.match_sender_url()));
    if (!matches) {
      return new Decision("ignored", event, null, false, false);
    }
    return new Decision(
        "ok", event, policyRepoUrl, event.equals(params.push_event_value()), false);
  }

  private Webhooks() {}

  /**
   * R120: the secret is either compared byte for byte or verified as an HMAC of the raw body.
   * The signature comparison is constant time, because a timing-variable one leaks the secret to
   * a caller willing to make enough requests.
   */
  public static void validateGitSecret(
      String webhookSecret,
      GitWebhookRequestParams params,
      String incomingHeaderValue,
      byte[] rawBody) {
    if (webhookSecret == null) {
      return;
    }
    String incoming = incomingHeaderValue == null ? "" : incomingHeaderValue;
    Matcher matcher = Pattern.compile(params.secret_parsing_regex()).matcher(incoming);
    String parsed = null;
    if (matcher.find()) {
      parsed = matcher.groupCount() >= 1 ? matcher.group(1) : matcher.group(0);
    }
    if (parsed == null || parsed.isEmpty()) {
      throw new Refused(401, "No secret was provided!");
    }
    if (params.secret_type() == SecretTypeEnum.signature) {
      String ourSignature =
          Hashing.hex(Aws.hmac(webhookSecret.getBytes(StandardCharsets.UTF_8), rawBody));
      if (!MessageDigest.isEqual(
          ourSignature.getBytes(StandardCharsets.UTF_8),
          parsed.getBytes(StandardCharsets.UTF_8))) {
        throw new Refused(401, "signatures didn't match!");
      }
      return;
    }
    if (!MessageDigest.isEqual(
        parsed.getBytes(StandardCharsets.UTF_8),
        webhookSecret.getBytes(StandardCharsets.UTF_8))) {
      throw new Refused(401, "secret-tokens didn't match!");
    }
  }

  /** R122 and R123: the nine URL fields, the two name fields, and the branch. */
  public static GitChanges extractGitChanges(JsonNode payload) {
    String ref = text(payload.path("ref"));
    if (ref == null) {
      ref = text(payload.path("refUpdates").path("name"));
    }
    String branch = null;
    if (ref != null) {
      branch = ref.startsWith("refs/heads/") ? ref.substring(11) : ref;
    }

    JsonNode repository = payload.path("repository");
    JsonNode project = payload.path("project");
    JsonNode azureRepository = payload.path("resource").path("repository");

    Set<String> urls = new LinkedHashSet<>();
    addIfPresent(urls, azureRepository.path("remoteUrl"));
    addIfPresent(urls, repository.path("git_url"));
    addIfPresent(urls, repository.path("ssh_url"));
    addIfPresent(urls, repository.path("clone_url"));
    addIfPresent(urls, repository.path("git_ssh_url"));
    addIfPresent(urls, repository.path("git_http_url"));
    addIfPresent(urls, repository.path("url"));
    addIfPresent(urls, project.path("git_http_url"));
    addIfPresent(urls, project.path("git_ssh_url"));

    Set<String> names = new LinkedHashSet<>();
    addIfPresent(names, project.path("path_with_namespace"));
    addIfPresent(names, repository.path("full_name"));

    if (urls.isEmpty() && names.isEmpty()) {
      throw new Refused(400, "repo url or full name not found in payload!");
    }
    return new GitChanges(new ArrayList<>(urls), branch, new ArrayList<>(names));
  }

  private static void addIfPresent(Set<String> into, JsonNode node) {
    String value = text(node);
    if (value != null) {
      into.add(value);
    }
  }

  private static String text(JsonNode node) {
    return node == null || node.isMissingNode() || node.isNull() ? null : node.asText();
  }

  /**
   * R122: the tracked URL is normalised — scheme, host, port and path kept, query and fragment
   * discarded — and compared against the URLs; where the payload carried none, the comparison
   * falls back to {@code owner/name}.
   */
  public static boolean isMatchingWebhookUrl(
      String inputUrl, List<String> urls, List<String> names) {
    URI parsed = URI.create(inputUrl);
    String host = parsed.getHost();
    String netloc = host == null ? "" : host;
    if (parsed.getPort() > 0) {
      netloc = host + ":" + parsed.getPort();
    }
    String path = parsed.getPath() == null ? "" : parsed.getPath();
    String normalized =
        (parsed.getScheme() == null ? "" : parsed.getScheme() + "://") + netloc + path;
    if (urls != null && !urls.isEmpty()) {
      return urls.contains(normalized);
    }
    String repoName = path.startsWith("/") ? path.substring(1) : path;
    if (repoName.endsWith(".git")) {
      repoName = repoName.substring(0, repoName.length() - 4);
    }
    return names != null && names.contains(repoName);
  }
}
