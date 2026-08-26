package io.akka.opal.client.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.opal.common.config.Enums.EngineLogFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * SPEC-002 R96 and OD-5 — the child process and what happens when it will not stay up.
 *
 * <p>The two callback sets are the point of the class. A first start hydrates an engine that has
 * never held anything; a restart hydrates one that held everything a moment ago and lost it, and
 * a runner that ran the first set on a restart would skip the re-fetch a restarted engine needs.
 */
class EngineRunnerTest {

  /** A runner over a command that does not exist, with the relaunch wait wound right down. */
  private static final class MissingBinaryRunner extends EngineRunner {
    MissingBinaryRunner() {
      super(EngineLogFormat.NONE);
      relaunchDelayMillis = 10;
    }

    @Override
    public String executablePath() {
      return "no-such-engine-binary-" + Long.toHexString(hashCode());
    }

    @Override
    public List<String> arguments() {
      return List.of("run");
    }

    @Override
    public boolean healthCheck() {
      return false;
    }

    @Override
    protected String engineName() {
      return "missing";
    }
  }

  /** A runner over a command that exists and exits at once, so a restart is reachable. */
  private static final class ShortLivedRunner extends EngineRunner {
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    ShortLivedRunner() {
      super(EngineLogFormat.NONE);
      relaunchDelayMillis = 10;
    }

    @Override
    public String executablePath() {
      return System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")
          ? "cmd.exe"
          : "sh";
    }

    @Override
    public List<String> arguments() {
      return System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")
          ? List.of("/c", "ping -n 2 127.0.0.1 > NUL")
          : List.of("-c", "sleep 0.3");
    }

    @Override
    public boolean healthCheck() {
      return healthy.get();
    }

    @Override
    protected String engineName() {
      return "short-lived";
    }
  }

  /**
   * OD-5: a binary that is not there is retried rather than raised. The client's start-up waits
   * on this, so the observable effect is a client that stays unready — which is what the source
   * does, and what a client whose engine has not arrived yet should do.
   */
  @Test
  void aMissingBinaryIsRetriedRatherThanRaised() throws Exception {
    try (MissingBinaryRunner runner = new MissingBinaryRunner()) {
      runner.start();
      assertFalse(
          runner.waitUntilReady(Duration.ofMillis(300)), "it never became ready");
      assertTrue(runner.launchAttempts() > 1, "and it kept trying: " + runner.launchAttempts());
    }
  }

  /** R96: the first start runs one callback set and a restart runs the other. */
  @Test
  void aRestartRunsTheRestartCallbacksAndNotTheFirstStartOnes() throws Exception {
    List<String> ran = new ArrayList<>();
    try (ShortLivedRunner runner = new ShortLivedRunner()) {
      runner.registerInitialStartCallbacks(List.of(() -> ran.add("first")));
      runner.registerRestartCallbacks(List.of(() -> ran.add("restart")));
      runner.start();
      assertTrue(runner.waitUntilReady(Duration.ofSeconds(10)), "the child came up");

      long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (ran.size() < 2 && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(List.of("first", "restart"), ran.subList(0, Math.min(2, ran.size())));
    }
  }
}
