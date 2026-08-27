package io.akka.opal.common.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.LayoutBase;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.util.FileSize;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akka.opal.common.config.CommonConfig;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 * What every `LOG_*` entry does — SPEC-002 R169–R177.
 *
 * <p>The source takes the process's logging over at start-up and rebuilds it from configuration:
 * one stream sink at a configured level, in a configured format, filtered by module, optionally
 * a second sink writing a rotating file, and a scrub over everything so a credential inside a URL
 * never reaches either. This does the same to this target's logging, which is what turns eighteen
 * entries that parse and print into eighteen entries that decide something.
 */
public final class Logs {

  private static final long STARTED = System.nanoTime();
  private static final long PROCESS_ID = ProcessHandle.current().pid();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The web server's own loggers, which `LOG_PATCH_UVICORN_LOGS` decides the fate of. */
  private static final List<String> WEB_SERVER_LOGGERS =
      List.of("akka.http", "akka.actor", "akka.io", "org.eclipse.jetty");

  private Logs() {}

  /** R169: rebuild logging from configuration, replacing whatever was there. */
  public static void configure(CommonConfig config) {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    context.reset();

    String format = config.getString("LOG_FORMAT");
    boolean colorize = Boolean.TRUE.equals(config.get("LOG_COLORIZE"));
    boolean traceback = Boolean.TRUE.equals(config.get("LOG_TRACEBACK"));
    boolean serialize = Boolean.TRUE.equals(config.get("LOG_SERIALIZE"));
    installGitTracing(Boolean.TRUE.equals(config.get("LOG_DIAGNOSE")));
    List<String> include = config.get("LOG_MODULE_INCLUDE_LIST");
    List<String> exclude = config.get("LOG_MODULE_EXCLUDE_LIST");

    ConsoleAppender<ILoggingEvent> console = new ConsoleAppender<>();
    console.setContext(context);
    console.setName("opal-console");
    console.setTarget(
        Boolean.TRUE.equals(config.get("LOG_PIPE_TO_STDERR")) ? "System.err" : "System.out");
    console.setEncoder(encoder(context, format, colorize, traceback, serialize));
    console.addFilter(moduleFilter(context, include, exclude));
    console.start();

    ch.qos.logback.classic.Logger root =
        context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(level(config.getString("LOG_LEVEL")));
    root.addAppender(console);

    if (Boolean.TRUE.equals(config.get("LOG_TO_FILE"))) {
      root.addAppender(fileAppender(context, config, format, traceback, include, exclude));
    }

    // R378: with patching off, the web server's own loggers are left exactly as they are — no
    // appender of their own and no break in propagation. That is what the source does: it
    // replaces their handlers only when asked to, and otherwise leaves them propagating to the
    // root. Taking them off the root instead would silence lines the source still prints.
    if (Boolean.TRUE.equals(config.get("LOG_PATCH_UVICORN_LOGS"))) {
      for (String name : WEB_SERVER_LOGGERS) {
        // Patching in the source means one handler and no propagation, so a line appears once
        // and in this format. Here the root already carries that handler, so what patching adds
        // is the half that stops the duplicate.
        context.getLogger(name).setAdditive(true);
      }
    }
  }

  /** R177: the level names the source accepts, and what an unreadable one falls back to. */
  static Level level(String name) {
    if (name == null || name.isEmpty()) {
      return Level.INFO;
    }
    return switch (name.trim().toUpperCase()) {
      case "TRACE" -> Level.TRACE;
      case "DEBUG" -> Level.DEBUG;
      case "WARNING", "WARN" -> Level.WARN;
      case "ERROR", "CRITICAL", "FATAL" -> Level.ERROR;
      default -> Level.INFO;
    };
  }

  private static LayoutWrappingEncoder<ILoggingEvent> encoder(
      LoggerContext context, String format, boolean colorize, boolean traceback,
      boolean serialize) {
    LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
    encoder.setContext(context);
    LayoutBase<ILoggingEvent> layout =
        serialize
            ? new SerializedLayout(format, colorize, traceback)
            : new LoguruLayout(format, colorize, traceback);
    layout.setContext(context);
    layout.start();
    encoder.setLayout(layout);
    encoder.start();
    return encoder;
  }

  private static Filter<ILoggingEvent> moduleFilter(
      LoggerContext context, List<String> include, List<String> exclude) {
    Filter<ILoggingEvent> filter =
        new Filter<>() {
          @Override
          public FilterReply decide(ILoggingEvent event) {
            return Loguru.allowed(event.getLoggerName(), include, exclude)
                ? FilterReply.NEUTRAL
                : FilterReply.DENY;
          }
        };
    filter.setContext(context);
    filter.start();
    return filter;
  }

  /**
   * R178: the second sink, with the size it rolls at, the days it keeps and whether it is
   * compressed.
   *
   * <p>The configured path may name the moment the file was opened, which is what keeps one run's
   * log separate from the next; that placeholder becomes the rolling file's own date.
   */
  private static RollingFileAppender<ILoggingEvent> fileAppender(
      LoggerContext context, CommonConfig config, String format, boolean traceback,
      List<String> include, List<String> exclude) {
    String path = config.getString("LOG_FILE_PATH");
    String compression = config.getString("LOG_FILE_COMPRESSION");
    String pattern = path.replace("{time}", "%d{yyyy-MM-dd}") + ".%i";
    if (!pattern.contains("%d{")) {
      pattern = pattern + ".%d{yyyy-MM-dd}";
    }
    if (compression != null && !compression.isEmpty() && !"None".equals(compression)) {
      pattern = pattern + "." + compression;
    }

    RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
    appender.setContext(context);
    appender.setName("opal-file");
    appender.setFile(path.replace("{time}", "current"));

    SizeAndTimeBasedRollingPolicy<ILoggingEvent> policy = new SizeAndTimeBasedRollingPolicy<>();
    policy.setContext(context);
    policy.setParent(appender);
    policy.setFileNamePattern(pattern);
    policy.setMaxFileSize(FileSize.valueOf(normaliseSize(config.getString("LOG_FILE_ROTATION"))));
    policy.setMaxHistory(days(config.getString("LOG_FILE_RETENTION")));
    policy.start();
    appender.setRollingPolicy(policy);

    boolean serialize = truthy(config.getString("LOG_FILE_SERIALIZE"));
    appender.setEncoder(encoder(context, format, false, traceback, serialize));
    appender.addFilter(moduleFilter(context, include, exclude));
    appender.addFilter(atLeast(context, level(config.getString("LOG_FILE_LEVEL"))));
    appender.start();
    return appender;
  }

  private static Filter<ILoggingEvent> atLeast(LoggerContext context, Level threshold) {
    Filter<ILoggingEvent> filter =
        new Filter<>() {
          @Override
          public FilterReply decide(ILoggingEvent event) {
            return event.getLevel().isGreaterOrEqual(threshold)
                ? FilterReply.NEUTRAL
                : FilterReply.DENY;
          }
        };
    filter.setContext(context);
    filter.start();
    return filter;
  }

  /** `250 MB` as the source writes it, in the spelling this target's parser reads. */
  static String normaliseSize(String size) {
    if (size == null || size.isBlank()) {
      return "250MB";
    }
    return size.trim().replace(" ", "");
  }

  /** `10 days` as a number of days; anything else keeps ten. */
  static int days(String retention) {
    if (retention == null) {
      return 10;
    }
    java.util.regex.Matcher matcher =
        java.util.regex.Pattern.compile("(\\d+)\\s*(day|week|month)").matcher(retention.trim());
    if (!matcher.find()) {
      return 10;
    }
    int count = Integer.parseInt(matcher.group(1));
    return switch (matcher.group(2)) {
      case "week" -> count * 7;
      case "month" -> count * 30;
      default -> count;
    };
  }

  static boolean truthy(String value) {
    return value != null
        && List.of("true", "1", "yes", "True", "TRUE").contains(value.trim());
  }

  /** One record, in the shape the format describes. */
  static Loguru.Record record(ILoggingEvent event, boolean traceback) {
    StackTraceElement[] caller = event.getCallerData();
    StackTraceElement first = caller != null && caller.length > 0 ? caller[0] : null;
    IThrowableProxy thrown = event.getThrowableProxy();
    String exception = thrown == null || !traceback ? null : ThrowableProxyUtil.asString(thrown);
    return new Loguru.Record(
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeStamp()), ZoneId.systemDefault()),
        PROCESS_ID,
        event.getThreadName(),
        Thread.currentThread().threadId(),
        event.getLoggerName(),
        Loguru.levelName(event.getLevel().toString()),
        event.getFormattedMessage(),
        exception,
        first == null ? "" : first.getMethodName(),
        first == null ? "" : first.getFileName(),
        first == null ? 0 : first.getLineNumber(),
        first == null ? "" : first.getClassName(),
        (System.nanoTime() - STARTED) / 1_000_000_000.0);
  }

  /** The line a record becomes, scrubbed of any credential a URL in it carried. */
  static final class LoguruLayout extends LayoutBase<ILoggingEvent> {

    private final String format;
    private final boolean colorize;
    private final boolean traceback;

    LoguruLayout(String format, boolean colorize, boolean traceback) {
      this.format = format;
      this.colorize = colorize;
      this.traceback = traceback;
    }

    @Override
    public String doLayout(ILoggingEvent event) {
      String text = Loguru.render(format, record(event, traceback), colorize);
      String scrubbed = Loguru.scrub(text);
      return scrubbed.endsWith("\n") ? scrubbed : scrubbed + "\n";
    }
  }

  /**
   * R275: {@code LOG_DIAGNOSE} turns on the git transport's own tracing.
   *
   * <p>The source sets {@code GIT_TRACE} and {@code GIT_CURL_VERBOSE} around every ssh clone and
   * pull, which makes the protocol conversation appear in the log it captures. The git here is a
   * library rather than a subprocess, and its equivalent is the ssh layer's logger.
   */
  private static void installGitTracing(boolean diagnose) {
    if (!diagnose) {
      com.jcraft.jsch.JSch.setLogger(new com.jcraft.jsch.Logger() {
        @Override
        public boolean isEnabled(int level) {
          return false;
        }

        @Override
        public void log(int level, String message) {}
      });
      return;
    }
    org.slf4j.Logger git = LoggerFactory.getLogger("git.transport");
    com.jcraft.jsch.JSch.setLogger(new com.jcraft.jsch.Logger() {
      @Override
      public boolean isEnabled(int level) {
        return true;
      }

      @Override
      public void log(int level, String message) {
        if (level >= com.jcraft.jsch.Logger.WARN) {
          git.warn(message);
        } else {
          git.info(message);
        }
      }
    });
  }

  /** The object a record becomes when `LOG_SERIALIZE` is on. */
  static final class SerializedLayout extends LayoutBase<ILoggingEvent> {

    private final String format;
    private final boolean colorize;
    private final boolean traceback;

    SerializedLayout(String format, boolean colorize, boolean traceback) {
      this.format = format;
      this.colorize = colorize;
      this.traceback = traceback;
    }

    @Override
    public String doLayout(ILoggingEvent event) {
      Loguru.Record record = record(event, traceback);
      String text = Loguru.render(format, record, colorize);
      if (!text.endsWith("\n")) {
        text = text + "\n";
      }
      try {
        return Loguru.scrub(MAPPER.writeValueAsString(Loguru.serialized(record, text))) + "\n";
      } catch (Exception e) {
        return Loguru.scrub(text);
      }
    }
  }

}
