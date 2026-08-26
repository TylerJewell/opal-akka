package io.akka.opal.client.engine;

import io.akka.opal.common.config.Enums.EngineLogFormat;
import io.akka.opal.common.config.Options.OpaServerOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An inline OPA — SPEC-002 R96. */
public final class OpaRunner extends EngineRunner {

  private static final Logger log = LoggerFactory.getLogger(OpaRunner.class);

  private final OpaServerOptions options;
  private final String execPath;
  private final String policyStoreUrl;
  private final double healthTimeoutSeconds;
  private final boolean v0Compatible;

  public OpaRunner(
      OpaServerOptions options,
      EngineLogFormat pipedLogsFormat,
      String execPath,
      String policyStoreUrl,
      double healthTimeoutSeconds,
      boolean v0Compatible) {
    super(pipedLogsFormat);
    this.options = options == null ? OpaServerOptions.defaults() : options;
    this.execPath = execPath;
    this.policyStoreUrl = policyStoreUrl;
    this.healthTimeoutSeconds = healthTimeoutSeconds;
    this.v0Compatible = v0Compatible;
  }

  @Override
  protected String engineName() {
    return "opa";
  }

  @Override
  public String executablePath() {
    if (execPath != null && !execPath.isEmpty()) {
      return execPath;
    }
    log.warn(
        "OPA executable path not set, looking for 'opa' binary in system PATH. It is recommended"
            + " to set the INLINE_OPA_EXEC_PATH configuration.");
    String path = which("opa");
    if (path == null) {
      throw new IllegalStateException("OPA executable not found in PATH");
    }
    return path;
  }

  @Override
  public List<String> arguments() {
    List<String> args = new ArrayList<>();
    args.add("run");
    args.add("--server");
    if (v0Compatible || Boolean.TRUE.equals(options.v0_compatible())) {
      args.add("--v0-compatible");
    }
    for (Map.Entry<String, String> option : options.cliOptions().entrySet()) {
      args.add(option.getKey() + "=" + option.getValue());
    }
    if (options.files() != null) {
      args.addAll(options.files());
    }
    return args;
  }

  @Override
  public boolean healthCheck() {
    return get(policyStoreUrl + "/health", healthTimeoutSeconds, List.of(200));
  }
}
