package io.akka.opal.client.engine;

import io.akka.opal.common.config.Enums.EngineLogFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a policy engine as a supervised child process — SPEC-002 R96 and OD-5.
 *
 * <p>Restarting it is not the whole job: a restarted engine has an empty cache, so the callbacks
 * registered for a restart are what re-hydrate it. The first start and every restart therefore
 * run different callbacks, and this keeps them apart.
 */
public abstract class EngineRunner implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(EngineRunner.class);

  private final EngineLogFormat pipedLogsFormat;
  private final List<Runnable> onInitialStart = new ArrayList<>();
  private final List<Runnable> onRestart = new ArrayList<>();
  private final AtomicBoolean shouldStop = new AtomicBoolean();
  private final CountDownLatch engineReady = new CountDownLatch(1);
  private final java.util.concurrent.atomic.AtomicInteger attempts =
      new java.util.concurrent.atomic.AtomicInteger();

  private volatile Process process;
  private volatile boolean neverUpBefore = true;
  private Thread supervisor;

  /**
   * R168: how long to wait before launching the child again, doubling from half a second to a
   * ceiling of ten.
   *
   * <p>A constant wait relaunches a crash-looping engine at a fixed rate forever; the source's
   * wait grows, so a repository misconfiguration that the engine will never accept costs a
   * launch every ten seconds rather than one a second.
   */
  public static long relaunchDelay(int attempt) {
    long millis = (long) (500 * Math.pow(2, Math.max(0, attempt - 1)));
    return Math.min(Math.max(millis, 500), MAX_RELAUNCH_DELAY_MILLIS);
  }

  public static final long MAX_RELAUNCH_DELAY_MILLIS = 10_000;

  /** Overridden by the tests, which cannot wait ten seconds to see a relaunch. */
  protected long relaunchDelayMillis = -1;

  private long nextRelaunchDelay(int attempt) {
    return relaunchDelayMillis >= 0 ? relaunchDelayMillis : relaunchDelay(attempt);
  }

  private volatile boolean enginePanicked;

  protected EngineRunner(EngineLogFormat pipedLogsFormat) {
    this.pipedLogsFormat = pipedLogsFormat == null ? EngineLogFormat.NONE : pipedLogsFormat;
  }

  public abstract String executablePath();

  public abstract List<String> arguments();

  public abstract boolean healthCheck();

  protected abstract String engineName();

  public void registerInitialStartCallbacks(List<Runnable> callbacks) {
    if (callbacks != null) {
      onInitialStart.addAll(callbacks);
    }
  }

  public void registerRestartCallbacks(List<Runnable> callbacks) {
    if (callbacks != null) {
      onRestart.addAll(callbacks);
    }
  }

  public void start() {
    if (supervisor != null) {
      return;
    }
    supervisor = new Thread(this::supervise, "opal-engine-" + engineName());
    supervisor.setDaemon(true);
    supervisor.start();
  }

  /**
   * Blocks until the engine answers its own health route.
   *
   * <p>There is no ceiling, because the launch is retried and the client has nothing to do
   * without the engine it is responsible for: a wait that gave up would leave a client reporting
   * itself started while every write it makes goes nowhere.
   */
  public void waitUntilReady() {
    try {
      engineReady.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Blocks until the engine answers its own health route, or the wait runs out. */
  public boolean waitUntilReady(Duration timeout) {
    try {
      return engineReady.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** How many times the child has been launched, counting the ones that never started. */
  public int launchAttempts() {
    return attempts.get();
  }

  private void supervise() {
    while (!shouldStop.get()) {
      try {
        attempts.incrementAndGet();
        runOnce();
      } catch (Exception e) {
        log.warn("policy engine run failed: {}", e.toString());
      }
      if (shouldStop.get()) {
        return;
      }
      try {
        Thread.sleep(nextRelaunchDelay(attempts.get()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void runOnce() throws Exception {
    enginePanicked = false;
    List<String> command = new ArrayList<>();
    command.add(executablePath());
    command.addAll(arguments());
    log.info("Running policy engine inline: {}", String.join(" ", command));

    ProcessBuilder builder = new ProcessBuilder(command);
    if (pipedLogsFormat == EngineLogFormat.NONE) {
      builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
      builder.redirectError(ProcessBuilder.Redirect.DISCARD);
    }
    process = builder.start();

    if (pipedLogsFormat != EngineLogFormat.NONE) {
      // R364: the two streams are read independently. Merged, a long line on one can be cut in
      // half by a line on the other, and the halves are then two lines that parse as neither —
      // which is how a panic trace stops matching the marker that terminates the engine.
      pipe(process.getInputStream(), "opal-engine-logs");
      pipe(process.getErrorStream(), "opal-engine-errors");
    }

    // R299: the health wait runs beside the engine, not in front of it. A failed wait leaves
    // the engine running and the readiness latch closed; it does not tear the process down and
    // start another, because an engine that is slow to answer is not an engine that failed.
    Thread health = new Thread(this::waitForHealthThenRunCallbacks, "opal-engine-health");
    health.setDaemon(true);
    health.start();

    int exitCode = process.waitFor();
    if (enginePanicked) {
      log.error("restart policy engine due to a detected panic");
    }
    log.info("Policy engine exited with return code: {}", exitCode);
    if (exitCode > 0 && !shouldStop.get()) {
      throw new IllegalStateException("Policy engine exited with return code: " + exitCode);
    }
  }

  private void waitForHealthThenRunCallbacks() {
    try {
      waitForHealth();
    } catch (RuntimeException e) {
      log.error("Engine failed health check: {}", e.getMessage());
      return;
    }
    engineReady.countDown();
    // R300: a second before the callbacks, which is what the source waits for the engine's own
    // API to be answering rather than merely listening.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    if (neverUpBefore) {
      neverUpBefore = false;
      log.info("Running policy engine initial start callbacks");
      onInitialStart.forEach(Runnable::run);
    } else {
      log.info("Running policy engine rehydration callbacks");
      onRestart.forEach(Runnable::run);
    }
  }

  /**
   * R301: thirty probes with a wait that doubles from a tenth of a second to two.
   *
   * <p>About fifty seconds in all, which is the window the source gives an engine to answer. A
   * fixed spacing reaches its last probe sooner and calls a slow engine unhealthy.
   */
  private void waitForHealth() {
    long waitMillis = 100;
    for (int attempt = 0; attempt < 30; attempt++) {
      log.debug("Checking policy engine health...");
      if (healthCheck()) {
        log.info("Policy engine is healthy and ready");
        return;
      }
      try {
        Thread.sleep(waitMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      waitMillis = Math.min(2000, waitMillis * 2);
    }
    throw new IllegalStateException("Policy engine not healthy yet");
  }

  /**
   * R163 and R167: renders one line of engine output and says whether it announced a panic.
   *
   * <p>A panicking engine writes a Go stack trace naming the runtime's own panic source file.
   * The source watches every line for that name because an engine that has panicked does not
   * necessarily exit — it can sit there holding its port — so the runner terminates it itself
   * and keeps reading, which is what puts the whole stack trace in the log before it dies.
   */
  private void pipe(java.io.InputStream stream, String name) {
    Thread piper =
        new Thread(
            () -> {
              try (BufferedReader reader =
                  new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                  boolean panicked = handleLogLine(line);
                  if (panicked && !enginePanicked) {
                    enginePanicked = true;
                    terminateEngine();
                  }
                }
              } catch (Exception e) {
                log.debug("engine log pipe closed: {}", e.toString());
              }
            },
            name);
    piper.setDaemon(true);
    piper.start();
  }

  /** R363: whether the rendered line is coloured, which follows the log configuration. */
  private volatile boolean colorize;

  public EngineRunner withColorize(boolean value) {
    this.colorize = value;
    return this;
  }

  protected boolean handleLogLine(String line) {
    EngineLogLine.Rendered rendered = EngineLogLine.render(line, pipedLogsFormat, colorize);
    if (rendered != null) {
      switch (EngineLogLine.severity(rendered.level())) {
        case "ERROR" -> log.error("{}", rendered.text());
        case "WARNING" -> log.warn("{}", rendered.text());
        case "DEBUG" -> log.debug("{}", rendered.text());
        default -> log.info("{}", rendered.text());
      }
    }
    return line.contains(PANIC_MARKER);
  }

  /** The file a Go runtime names in the stack trace it writes when it panics. */
  public static final String PANIC_MARKER = "go/src/runtime/panic.go";

  /**
   * R169: stops the engine and everything it started.
   *
   * <p>An engine launched through a shell has the shell as the child and the engine as its
   * grandchild, so destroying the direct child leaves the engine holding its port. The source
   * kills the process group for that reason; the descendants of the child are the same set here.
   */
  private void terminateEngine() {
    Process running = process;
    if (running == null || !running.isAlive()) {
      return;
    }
    log.info("Stopping policy engine");
    // R303: asked to stop, and not forced. The source signals the process group once and waits a
    // second; it never escalates, so an engine that ignores the signal keeps running and the
    // caller sees that rather than a kill it did not ask for.
    running.descendants().forEach(ProcessHandle::destroy);
    running.destroy();
    try {
      running.waitFor(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    shouldStop.set(true);
    terminateEngine();
    if (supervisor != null) {
      supervisor.interrupt();
      supervisor = null;
    }
  }

  /** Finds a binary on the PATH the way the source does when no explicit path is configured. */
  protected static String which(String name) {
    String path = System.getenv("PATH");
    if (path == null) {
      return null;
    }
    String[] extensions =
        System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")
            ? new String[] {".exe", ".cmd", ".bat", ""}
            : new String[] {""};
    for (String directory : path.split(java.io.File.pathSeparator)) {
      for (String extension : extensions) {
        Path candidate = Path.of(directory, name + extension);
        if (Files.isExecutable(candidate)) {
          return candidate.toString();
        }
      }
    }
    return null;
  }

  protected static boolean get(String url, double timeoutSeconds, List<Integer> accepted) {
    try {
      HttpResponse<Void> response =
          io.akka.opal.common.util.Http.plain()
              .send(
                  HttpRequest.newBuilder(URI.create(url))
                      .timeout(Duration.ofMillis((long) (timeoutSeconds * 1000)))
                      .GET()
                      .build(),
                  HttpResponse.BodyHandlers.discarding());
      return accepted.contains(response.statusCode());
    } catch (Exception e) {
      return false;
    }
  }
}
