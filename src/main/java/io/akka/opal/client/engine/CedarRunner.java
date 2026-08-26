package io.akka.opal.client.engine;

import io.akka.opal.common.config.Enums.EngineLogFormat;
import io.akka.opal.common.config.Options.CedarServerOptions;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An inline Cedar agent — SPEC-002 R96. */
public final class CedarRunner extends EngineRunner {

  private static final Logger log = LoggerFactory.getLogger(CedarRunner.class);

  private final CedarServerOptions options;
  private final String execPath;
  private final String policyStoreUrl;
  private final double healthTimeoutSeconds;

  public CedarRunner(
      CedarServerOptions options,
      EngineLogFormat pipedLogsFormat,
      String execPath,
      String policyStoreUrl,
      double healthTimeoutSeconds) {
    super(pipedLogsFormat);
    this.options = options == null ? CedarServerOptions.defaults() : options;
    this.execPath = execPath;
    this.policyStoreUrl = policyStoreUrl;
    this.healthTimeoutSeconds = healthTimeoutSeconds;
  }

  @Override
  protected String engineName() {
    return "cedar";
  }

  @Override
  public String executablePath() {
    if (execPath != null && !execPath.isEmpty()) {
      return execPath;
    }
    log.warn(
        "Cedar executable path not set, looking for 'cedar-agent' binary in system PATH. It is"
            + " recommended to set the INLINE_CEDAR_EXEC_PATH configuration.");
    String path = which("cedar-agent");
    if (path == null) {
      throw new IllegalStateException("Cedar agent executable not found in PATH");
    }
    return path;
  }

  @Override
  public List<String> arguments() {
    return options.args();
  }

  @Override
  public boolean healthCheck() {
    return get(policyStoreUrl + "/v1/", healthTimeoutSeconds, List.of(200, 204));
  }
}
