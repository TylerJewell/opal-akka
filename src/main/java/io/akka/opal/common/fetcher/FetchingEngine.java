package io.akka.opal.common.fetcher;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded queue of fetches worked by a fixed set of threads — SPEC-002 R149.
 *
 * <p>The bound is the point. A data update naming two hundred sources would otherwise open two
 * hundred connections at once against whatever is on the other end, and the systems OPAL fetches
 * from belong to somebody else.
 */
public final class FetchingEngine implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(FetchingEngine.class);

  public static final int DEFAULT_WORKER_COUNT = 6;
  public static final int DEFAULT_CALLBACK_TIMEOUT_SECONDS = 10;
  public static final int DEFAULT_ENQUEUE_TIMEOUT_SECONDS = 10;

  private record Queued(FetchEvent event, Consumer<JsonNode> callback) {}

  private final FetcherRegister register;
  /**
   * Unbounded, because the source's is. A cap would refuse a data update carrying more entries
   * than the cap allows, which is a limit the source does not have and a caller could hit.
   */
  private final BlockingQueue<Queued> queue = new LinkedBlockingQueue<>();
  private final List<Thread> workers = new ArrayList<>();
  private final List<BiConsumer<Exception, FetchEvent>> failureHandlers = new ArrayList<>();
  private final int workerCount;
  private final int callbackTimeoutSeconds;
  private final int enqueueTimeoutSeconds;

  private volatile boolean running;

  /**
   * R179: the wait between fetch attempts, replaced by the tests.
   *
   * <p>Two hundred attempts is what the source allows, and a test that had to serve them at their
   * real spacing would take longer than the age of the machine.
   */
  private Retries.Sleeper sleeper = Retries.REAL;

  /**
   * R262: what a fetch retries on when the event asks for nothing in particular.
   *
   * <p>The client builds its engine from {@code DATA_UPDATER_CONN_RETRY}, so a deployment that
   * wants three quick attempts rather than two hundred slow ones says so once, for every source
   * it fetches, rather than on each entry.
   */
  private Retries.Config defaultRetry = Retries.Config.defaults();

  public FetchingEngine withDefaultRetry(Retries.Config policy) {
    if (policy != null) {
      this.defaultRetry = policy;
    }
    return this;
  }

  void setSleeper(Retries.Sleeper replacement) {
    this.sleeper = replacement;
  }

  public FetchingEngine(HttpClient http, double httpTimeoutSeconds) {
    this(
        new FetcherRegister(http, httpTimeoutSeconds),
        DEFAULT_WORKER_COUNT,
        DEFAULT_CALLBACK_TIMEOUT_SECONDS,
        DEFAULT_ENQUEUE_TIMEOUT_SECONDS);
  }

  public FetchingEngine(
      FetcherRegister register,
      int workerCount,
      int callbackTimeoutSeconds,
      int enqueueTimeoutSeconds) {
    this.register = register;
    this.workerCount = workerCount;
    this.callbackTimeoutSeconds = callbackTimeoutSeconds;
    this.enqueueTimeoutSeconds = enqueueTimeoutSeconds;
  }

  public FetcherRegister register() {
    return register;
  }

  public synchronized void startWorkers() {
    if (running) {
      return;
    }
    running = true;
    for (int i = 0; i < workerCount; i++) {
      Thread worker = new Thread(this::work, "opal-fetch-worker-" + i);
      worker.setDaemon(true);
      worker.start();
      workers.add(worker);
    }
  }

  public void registerFailureHandler(BiConsumer<Exception, FetchEvent> handler) {
    failureHandlers.add(handler);
  }

  private void work() {
    while (running) {
      Queued queued;
      try {
        queued = queue.poll(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (queued == null) {
        continue;
      }
      try {
        FetchProvider provider = register.getFetcherForEvent(queued.event());
        JsonNode data;
        Retries.Config policy =
            queued.event().retry() == null
                ? defaultRetry
                : Retries.Config.from(queued.event().retry());
        try (FetchProvider opened = provider.open()) {
          Object fetched = Retries.call(policy, opened::fetch, sleeper);
          try {
            data = opened.process(fetched);
          } catch (Exception e) {
            // The source names the stage: a provider that fetched and could not make sense of
            // what came back is a different fault from one that could not fetch.
            log.error("Failed to process fetched data", e);
            throw e;
          }
        }
        try {
          queued.callback().accept(data);
        } catch (Exception e) {
          log.error("Fetcher callback failed", e);
          onFailure(e, queued.event());
        }
      } catch (Exception e) {
        log.error("Failed to process fetch event", e);
        onFailure(e, queued.event());
      }
    }
  }

  private void onFailure(Exception error, FetchEvent event) {
    for (BiConsumer<Exception, FetchEvent> handler : failureHandlers) {
      try {
        handler.accept(error, event);
      } catch (Exception e) {
        log.error("A fetch failure handler itself failed", e);
      }
    }
  }

  /**
   * R150: a configuration naming a fetcher overrides the caller's default, because the caller
   * chose a default and the configuration was written by whoever knows where the data is.
   */
  public FetchEvent queueUrl(
      String url, Consumer<JsonNode> callback, Map<String, Object> config, String fetcher) {
    String chosen = fetcher == null ? FetchEvent.DEFAULT_FETCHER : fetcher;
    if (config != null && config.get("fetcher") instanceof String named && !named.isEmpty()) {
      chosen = named;
    }
    return queueFetchEvent(new FetchEvent(url, chosen, config, null), callback);
  }

  /** R151: every queued event is given an id. */
  public FetchEvent queueFetchEvent(FetchEvent event, Consumer<JsonNode> callback) {
    startWorkers();
    event.setId(UUID.randomUUID().toString().replace("-", ""));
    // R272: an absent timeout means do not wait at all — the source picks its no-wait put in
    // that case, and on an unbounded queue neither ever blocks.
    if (enqueueTimeoutSeconds <= 0) {
      if (!queue.offer(new Queued(event, callback))) {
        throw new IllegalStateException("fetch queue is full");
      }
      return event;
    }
    try {
      if (!queue.offer(new Queued(event, callback), enqueueTimeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("fetch queue is full");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while queueing a fetch", e);
    }
    return event;
  }

  /**
   * Queue a fetch and wait for its answer, which is what a caller with nowhere to put a callback
   * needs. A failure on the queued event is re-raised here rather than only reaching the failure
   * handlers, so a caller waiting on an answer is told there will not be one.
   */
  public JsonNode handleUrl(String url, Map<String, Object> config, String fetcher) {
    AtomicReference<JsonNode> result = new AtomicReference<>();
    AtomicReference<Exception> failure = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);

    BiConsumer<Exception, FetchEvent> handler =
        (error, event) -> {
          failure.set(error);
          done.countDown();
        };
    failureHandlers.add(handler);
    try {
      queueUrl(
          url,
          data -> {
            result.set(data);
            done.countDown();
          },
          config,
          fetcher);
      if (!done.await(callbackTimeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
            "timed out fetching " + io.akka.opal.common.util.Urls.redactUrl(url));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while fetching", e);
    } finally {
      failureHandlers.remove(handler);
    }
    Exception error = failure.get();
    if (error != null) {
      throw error instanceof RuntimeException runtime
          ? runtime
          : new IllegalStateException(error);
    }
    return result.get();
  }

  public void terminateWorkers() {
    running = false;
    workers.forEach(Thread::interrupt);
    workers.clear();
  }

  @Override
  public void close() {
    terminateWorkers();
  }
}
