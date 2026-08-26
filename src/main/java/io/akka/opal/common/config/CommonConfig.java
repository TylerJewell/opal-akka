package io.akka.opal.common.config;

import io.akka.opal.common.auth.Types.EncryptionKeyFormat;
import io.akka.opal.common.auth.Types.JWTAlgorithm;
import io.akka.opal.common.confi.Confi;
import io.akka.opal.common.confi.ConfiEntry;
import java.util.List;
import java.util.Map;

/**
 * The 40 entries both processes share. SPEC-002 R6.
 *
 * <p>Generated from the census taken off the running source by
 * {@code opal-port/probes/complete/gen_config.py}, so that the set is the source's own
 * rather than a transcription of it. Declaration order is the source's declaration
 * order, which is also evaluation order.
 */
public final class CommonConfig extends Confi {

  static final String LOG_FORMAT_WITHOUT_PID =
      "<green>{time}</green> | <blue>{name: <40}</blue>|<level>{level:^6} | {message}</level>\n{exception}";
  static final String LOG_FORMAT_WITH_PID =
      "<green>{time}</green> | {process} | <blue>{name: <40}</blue>|<level>{level:^6} | {message}</level>\n{exception}";

  public CommonConfig(Map<String, String> environment) {
    super("OPAL_", environment);
    declare();
    resolveDelayed();
  }

  public CommonConfig() {
    this(System.getenv());
  }

  private void declare() {
    list("ALLOWED_ORIGINS", List.of("*"), "List of allowed origins for CORS");
    bool("LOG_FORMAT_INCLUDE_PID", false, "Include process ID in log format");
    strDelayed("LOG_FORMAT", c -> Boolean.TRUE.equals(c.get("LOG_FORMAT_INCLUDE_PID"))
            ? LOG_FORMAT_WITH_PID
            : LOG_FORMAT_WITHOUT_PID, "The format of the log messages");
    bool("LOG_TRACEBACK", true, "Include traceback in log messages");
    bool("LOG_DIAGNOSE", false, "Include diagnosis (local variable values) in tracebacks. Off by default because loguru renders raw variable values, which can leak credentials (e.g. auth headers/tokens) into logs - only enable for local debugging. Also gates verbose git SSH protocol tracing (GIT_TRACE/GIT_CURL_VERBOSE on SSH clones), which adds noisy protocol/host disclosure to logs.");
    bool("LOG_COLORIZE", true, "Colorize log messages");
    bool("LOG_SERIALIZE", false, "Serialize log messages");
    bool("LOG_PIPE_TO_STDERR", true, "Should we send logs to stderr? otherwise to stdout");
    bool("LOG_SHOW_CODE_LINE", true, "Show code line in log messages");
    str("LOG_LEVEL", "INFO", "The log level to show");
    list("LOG_MODULE_EXCLUDE_LIST", List.of("uvicorn"), "List of modules to exclude from logging");
    list("LOG_MODULE_INCLUDE_LIST", List.of("uvicorn.protocols.http"), "List of modules to include in logging");
    bool("LOG_PATCH_UVICORN_LOGS", true, "Should we takeover UVICORN's logs so they appear in the main logger");
    bool("LOG_TO_FILE", false, "Should we log to a file");
    str("LOG_FILE_PATH", "opal_{time}.log", "path to save log file");
    str("LOG_FILE_ROTATION", "250 MB", "Log file rotation size");
    str("LOG_FILE_RETENTION", "10 days", "Log file retention time");
    str("LOG_FILE_COMPRESSION", null, "Log file compression format");
    str("LOG_FILE_SERIALIZE", "True", "Serialize log messages in file");
    str("LOG_FILE_LEVEL", "INFO", "The log level to show in file");
    bool("STATISTICS_ENABLED", false, "Set if OPAL server will collect statistics about OPAL clients may cause a small performance hit");
    str("STATISTICS_ADD_CLIENT_CHANNEL", "__opal_stats_add", "The topic to update about new OPAL clients connection");
    str("STATISTICS_REMOVE_CLIENT_CHANNEL", "__opal_stats_rm", "The topic to update about OPAL clients disconnection");
    list("FETCH_PROVIDER_MODULES", List.of("opal_common.fetcher.providers"), "List of modules to load fetch providers from");
    integer("FETCHING_WORKER_COUNT", 6, "Max number of worker tasks handling fetch events concurrently");
    integer("FETCHING_CALLBACK_TIMEOUT", 10, "Time in seconds to wait on the queued fetch task");
    integer("FETCHING_ENQUEUE_TIMEOUT", 10, "Time in seconds to wait for queuing a new task (if the queue is full)");
    str("GIT_SSH_KEY_FILE",
            System.getProperty("user.home") + "/.ssh/opal_repo_ssh_key", "Path to the SSH key file for Git");
    bool("CLIENT_SELF_SIGNED_CERTIFICATES_ALLOWED", false, "Whether or not OPAL Client will trust HTTPs connections protected by self signed certificates. DO NOT USE THIS IN PRODUCTION!");
    str("CLIENT_SSL_CONTEXT_TRUSTED_CA_FILE", null, "A path to your own CA public certificate file (usually a .crt or a .pem file). Certificates signed by this issuer will be trusted by OPAL Client. DO NOT USE THIS IN PRODUCTION!");
    enumeration("AUTH_PUBLIC_KEY_FORMAT", EncryptionKeyFormat.class, EncryptionKeyFormat.ssh, "Format of the public key for authentication");
    key("AUTH_PUBLIC_KEY", "Public key for authentication",
            () -> get("AUTH_PUBLIC_KEY_FORMAT"));
    enumeration("AUTH_JWT_ALGORITHM", JWTAlgorithm.class, JWTAlgorithm.RS256, "jwt algorithm, possible values: see: https://pyjwt.readthedocs.io/en/stable/algorithms.html");
    str("AUTH_JWT_AUDIENCE", "https://api.opal.ac/v1/", "Audience for JWT authentication");
    str("AUTH_JWT_ISSUER", "https://opal.ac/", "Issuer for JWT authentication");
    list("POLICY_REPO_POLICY_EXTENSIONS", List.of(".rego"), "List of extensions to serve as policy modules");
    bool("ENABLE_METRICS", false, "Enable metrics collection");
    bool("ENABLE_DATADOG_APM", false, "Set if OPAL server should enable tracing with datadog APM");
    str("HTTP_FETCHER_PROVIDER_CLIENT", "aiohttp", "The client to use for fetching data, can be either aiohttp or httpx.if provided different value, aiohttp will be used.");
    decimal("HTTP_FETCHER_TIMEOUT", 5.0, "The timeout for the httpx or aiohttp fetcher provider, in seconds. if provided different value, 5 seconds will be used.");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> ALLOWED_ORIGINS() {
    return (ConfiEntry<List<String>>) entries().get("ALLOWED_ORIGINS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_FORMAT_INCLUDE_PID() {
    return (ConfiEntry<Boolean>) entries().get("LOG_FORMAT_INCLUDE_PID");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FORMAT() {
    return (ConfiEntry<String>) entries().get("LOG_FORMAT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_TRACEBACK() {
    return (ConfiEntry<Boolean>) entries().get("LOG_TRACEBACK");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_DIAGNOSE() {
    return (ConfiEntry<Boolean>) entries().get("LOG_DIAGNOSE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_COLORIZE() {
    return (ConfiEntry<Boolean>) entries().get("LOG_COLORIZE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_SERIALIZE() {
    return (ConfiEntry<Boolean>) entries().get("LOG_SERIALIZE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_PIPE_TO_STDERR() {
    return (ConfiEntry<Boolean>) entries().get("LOG_PIPE_TO_STDERR");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_SHOW_CODE_LINE() {
    return (ConfiEntry<Boolean>) entries().get("LOG_SHOW_CODE_LINE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_LEVEL() {
    return (ConfiEntry<String>) entries().get("LOG_LEVEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> LOG_MODULE_EXCLUDE_LIST() {
    return (ConfiEntry<List<String>>) entries().get("LOG_MODULE_EXCLUDE_LIST");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> LOG_MODULE_INCLUDE_LIST() {
    return (ConfiEntry<List<String>>) entries().get("LOG_MODULE_INCLUDE_LIST");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_PATCH_UVICORN_LOGS() {
    return (ConfiEntry<Boolean>) entries().get("LOG_PATCH_UVICORN_LOGS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> LOG_TO_FILE() {
    return (ConfiEntry<Boolean>) entries().get("LOG_TO_FILE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FILE_PATH() {
    return (ConfiEntry<String>) entries().get("LOG_FILE_PATH");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FILE_ROTATION() {
    return (ConfiEntry<String>) entries().get("LOG_FILE_ROTATION");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FILE_RETENTION() {
    return (ConfiEntry<String>) entries().get("LOG_FILE_RETENTION");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FILE_COMPRESSION() {
    return (ConfiEntry<String>) entries().get("LOG_FILE_COMPRESSION");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FILE_SERIALIZE() {
    return (ConfiEntry<String>) entries().get("LOG_FILE_SERIALIZE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> LOG_FILE_LEVEL() {
    return (ConfiEntry<String>) entries().get("LOG_FILE_LEVEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> STATISTICS_ENABLED() {
    return (ConfiEntry<Boolean>) entries().get("STATISTICS_ENABLED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STATISTICS_ADD_CLIENT_CHANNEL() {
    return (ConfiEntry<String>) entries().get("STATISTICS_ADD_CLIENT_CHANNEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> STATISTICS_REMOVE_CLIENT_CHANNEL() {
    return (ConfiEntry<String>) entries().get("STATISTICS_REMOVE_CLIENT_CHANNEL");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> FETCH_PROVIDER_MODULES() {
    return (ConfiEntry<List<String>>) entries().get("FETCH_PROVIDER_MODULES");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> FETCHING_WORKER_COUNT() {
    return (ConfiEntry<Integer>) entries().get("FETCHING_WORKER_COUNT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> FETCHING_CALLBACK_TIMEOUT() {
    return (ConfiEntry<Integer>) entries().get("FETCHING_CALLBACK_TIMEOUT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Integer> FETCHING_ENQUEUE_TIMEOUT() {
    return (ConfiEntry<Integer>) entries().get("FETCHING_ENQUEUE_TIMEOUT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> GIT_SSH_KEY_FILE() {
    return (ConfiEntry<String>) entries().get("GIT_SSH_KEY_FILE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> CLIENT_SELF_SIGNED_CERTIFICATES_ALLOWED() {
    return (ConfiEntry<Boolean>) entries().get("CLIENT_SELF_SIGNED_CERTIFICATES_ALLOWED");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> CLIENT_SSL_CONTEXT_TRUSTED_CA_FILE() {
    return (ConfiEntry<String>) entries().get("CLIENT_SSL_CONTEXT_TRUSTED_CA_FILE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<EncryptionKeyFormat> AUTH_PUBLIC_KEY_FORMAT() {
    return (ConfiEntry<EncryptionKeyFormat>) entries().get("AUTH_PUBLIC_KEY_FORMAT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_PUBLIC_KEY() {
    return (ConfiEntry<String>) entries().get("AUTH_PUBLIC_KEY");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<JWTAlgorithm> AUTH_JWT_ALGORITHM() {
    return (ConfiEntry<JWTAlgorithm>) entries().get("AUTH_JWT_ALGORITHM");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_JWT_AUDIENCE() {
    return (ConfiEntry<String>) entries().get("AUTH_JWT_AUDIENCE");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> AUTH_JWT_ISSUER() {
    return (ConfiEntry<String>) entries().get("AUTH_JWT_ISSUER");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<List<String>> POLICY_REPO_POLICY_EXTENSIONS() {
    return (ConfiEntry<List<String>>) entries().get("POLICY_REPO_POLICY_EXTENSIONS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> ENABLE_METRICS() {
    return (ConfiEntry<Boolean>) entries().get("ENABLE_METRICS");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Boolean> ENABLE_DATADOG_APM() {
    return (ConfiEntry<Boolean>) entries().get("ENABLE_DATADOG_APM");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<String> HTTP_FETCHER_PROVIDER_CLIENT() {
    return (ConfiEntry<String>) entries().get("HTTP_FETCHER_PROVIDER_CLIENT");
  }

  @SuppressWarnings("unchecked")
  public ConfiEntry<Double> HTTP_FETCHER_TIMEOUT() {
    return (ConfiEntry<Double>) entries().get("HTTP_FETCHER_TIMEOUT");
  }

}
