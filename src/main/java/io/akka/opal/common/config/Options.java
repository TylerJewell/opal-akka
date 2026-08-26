package io.akka.opal.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The structured values a handful of configuration entries carry. */
public final class Options {

  private Options() {}

  public enum WaitStrategy {
    fixed,
    exponential,
    random_exponential;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static WaitStrategy from(String value) {
      return valueOf(value.toLowerCase());
    }
  }

  /** Tenacity's default ceiling, which OPAL adopts by naming {@code _utils.MAX_WAIT}. */
  public static final double MAX_WAIT = 4.611686018427388e18;

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record ConnRetryOptions(
      WaitStrategy wait_strategy, Double wait_time, Integer attempts, Double max_wait)
      implements io.akka.opal.common.util.Repr.Reprable {
    public ConnRetryOptions {
      if (wait_strategy == null) {
        wait_strategy = WaitStrategy.fixed;
      }
      if (wait_time == null) {
        wait_time = 2.0;
      }
      if (attempts == null) {
        attempts = 2;
      }
      if (max_wait == null) {
        max_wait = MAX_WAIT;
      }
    }

    public static ConnRetryOptions defaults() {
      return new ConnRetryOptions(null, null, null, null);
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "wait_strategy=" + io.akka.opal.common.util.Repr.repr(wait_strategy),
          "wait_time=" + io.akka.opal.common.util.Repr.repr(wait_time),
          "attempts=" + io.akka.opal.common.util.Repr.repr(attempts),
          "max_wait=" + io.akka.opal.common.util.Repr.repr(max_wait));
    }

    /** The wait before attempt {@code n}, counting from one, in milliseconds. */
    public long waitMillis(int attempt, java.util.random.RandomGenerator random) {
      double seconds =
          switch (wait_strategy) {
            case fixed -> wait_time;
            case exponential -> Math.min(wait_time * Math.pow(2, attempt - 1), max_wait);
            case random_exponential ->
                random.nextDouble() * Math.min(wait_time * Math.pow(2, attempt - 1), max_wait);
          };
      return (long) (seconds * 1000);
    }
  }

  public enum LogLevel {
    info,
    debug,
    error;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static LogLevel from(String value) {
      return valueOf(value.toLowerCase());
    }
  }

  public enum AuthenticationScheme {
    off,
    token,
    tls;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static AuthenticationScheme from(String value) {
      return valueOf(value.toLowerCase());
    }
  }

  public enum AuthorizationScheme {
    off,
    basic;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static AuthorizationScheme from(String value) {
      return valueOf(value.toLowerCase());
    }
  }

  /**
   * The arguments an inline OPA is started with. The command-line names are the field names
   * with underscores turned into hyphens and a {@code --} in front, which is how OPAL derives
   * them from the same model.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record OpaServerOptions(
      String addr,
      AuthenticationScheme authentication,
      AuthorizationScheme authorization,
      String config_file,
      String tls_ca_cert_file,
      String tls_cert_file,
      String tls_private_key_file,
      String tls_cert_refresh_period,
      LogLevel log_level,
      List<String> files,
      Boolean v0_compatible)
      implements io.akka.opal.common.util.Repr.Reprable {

    public OpaServerOptions {
      if (addr == null) {
        addr = "0.0.0.0:8181";
      }
      if (authentication == null) {
        authentication = AuthenticationScheme.off;
      }
      if (authorization == null) {
        authorization = AuthorizationScheme.off;
      }
      if (log_level == null) {
        log_level = LogLevel.info;
      }
      if (v0_compatible == null) {
        v0_compatible = false;
      }
    }

    public static OpaServerOptions defaults() {
      return new OpaServerOptions(null, null, null, null, null, null, null, null, null, null, null);
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "addr=" + io.akka.opal.common.util.Repr.repr(addr),
          "authentication=" + io.akka.opal.common.util.Repr.repr(authentication.wire()),
          "authorization=" + io.akka.opal.common.util.Repr.repr(authorization.wire()),
          "config_file=" + io.akka.opal.common.util.Repr.repr(config_file),
          "tls_ca_cert_file=" + io.akka.opal.common.util.Repr.repr(tls_ca_cert_file),
          "tls_cert_file=" + io.akka.opal.common.util.Repr.repr(tls_cert_file),
          "tls_private_key_file=" + io.akka.opal.common.util.Repr.repr(tls_private_key_file),
          "tls_cert_refresh_period=" + io.akka.opal.common.util.Repr.repr(tls_cert_refresh_period),
          "log_level=" + io.akka.opal.common.util.Repr.repr(log_level.wire()),
          "files=" + io.akka.opal.common.util.Repr.repr(files),
          "v0_compatible=" + io.akka.opal.common.util.Repr.repr(v0_compatible));
    }

    /** {@code files} and {@code v0_compatible} are excluded: neither is a CLI option. */
    public Map<String, String> cliOptions() {
      Map<String, String> out = new LinkedHashMap<>();
      out.put("--addr", addr);
      out.put("--authentication", authentication.wire());
      out.put("--authorization", authorization.wire());
      putIfPresent(out, "--config-file", config_file);
      putIfPresent(out, "--tls-ca-cert-file", tls_ca_cert_file);
      putIfPresent(out, "--tls-cert-file", tls_cert_file);
      putIfPresent(out, "--tls-private-key-file", tls_private_key_file);
      putIfPresent(out, "--tls-cert-refresh-period", tls_cert_refresh_period);
      out.put("--log-level", log_level.wire());
      return out;
    }

    private static void putIfPresent(Map<String, String> out, String key, String value) {
      if (value != null) {
        out.put(key, value);
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record CedarServerOptions(
      String addr,
      AuthenticationScheme authentication,
      String authentication_token,
      List<String> files)
      implements io.akka.opal.common.util.Repr.Reprable {

    private static final Pattern HOST_ADDR =
        Pattern.compile("^(?<addr>(?:\\d{1,3}\\.){3}\\d{1,3}|)(?::(?<port>\\d+))?$");

    public CedarServerOptions {
      if (addr == null) {
        addr = "0.0.0.0:8180";
      }
      if (authentication == null) {
        authentication = AuthenticationScheme.off;
      }
      if (authentication == AuthenticationScheme.tls) {
        throw new IllegalArgumentException("Invalid AuthenticationScheme for Cedar.");
      }
      if (authentication == AuthenticationScheme.token && authentication_token == null) {
        throw new IllegalArgumentException(
            "A token must be specified for AuthenticationScheme.token.");
      }
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "addr=" + io.akka.opal.common.util.Repr.repr(addr),
          "authentication=" + io.akka.opal.common.util.Repr.repr(authentication.wire()),
          "authentication_token=" + io.akka.opal.common.util.Repr.repr(authentication_token),
          "files=" + io.akka.opal.common.util.Repr.repr(files));
    }

    public static CedarServerOptions defaults() {
      return new CedarServerOptions(null, null, null, null);
    }

    public List<String> args() {
      List<String> out = new ArrayList<>();
      if (authentication == AuthenticationScheme.token && authentication_token != null) {
        out.add("-a");
        out.add(authentication_token);
      }
      Matcher match = HOST_ADDR.matcher(addr);
      if (!match.matches()) {
        throw new IllegalArgumentException(
            "Invalid addr format: " + addr + ". Expected [ip]:<port>, e.g. '0.0.0.0:8180', ':8180'");
      }
      String host = match.group("addr");
      String port = match.group("port");
      out.add("--addr");
      out.add(host == null || host.isEmpty() ? "0.0.0.0" : host);
      out.add("--port");
      out.add(port == null || port.isEmpty() ? "8180" : port);
      return out;
    }
  }

  public enum SecretTypeEnum {
    token,
    signature;

    @JsonValue
    public String wire() {
      return name();
    }

    @JsonCreator
    public static SecretTypeEnum from(String value) {
      return valueOf(value.toLowerCase());
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record GitWebhookRequestParams(
      String secret_header_name,
      SecretTypeEnum secret_type,
      String secret_parsing_regex,
      String event_header_name,
      String event_request_key,
      String push_event_value,
      Boolean match_sender_url)
      implements io.akka.opal.common.util.Repr.Reprable {

    public GitWebhookRequestParams {
      if (match_sender_url == null) {
        match_sender_url = true;
      }
    }

    @Override
    public List<String> pyFields() {
      return List.of(
          "secret_header_name=" + io.akka.opal.common.util.Repr.repr(secret_header_name),
          "secret_type=" + io.akka.opal.common.util.Repr.repr(secret_type),
          "secret_parsing_regex=" + io.akka.opal.common.util.Repr.repr(secret_parsing_regex),
          "event_header_name=" + io.akka.opal.common.util.Repr.repr(event_header_name),
          "event_request_key=" + io.akka.opal.common.util.Repr.repr(event_request_key),
          "push_event_value=" + io.akka.opal.common.util.Repr.repr(push_event_value),
          "match_sender_url=" + io.akka.opal.common.util.Repr.repr(match_sender_url));
    }

    /** GitHub's shape, which is what OPAL ships as the default. */
    public static GitWebhookRequestParams github() {
      return new GitWebhookRequestParams(
          "x-hub-signature-256",
          SecretTypeEnum.signature,
          "sha256=(.*)",
          "X-GitHub-Event",
          null,
          "push",
          true);
    }
  }
}
