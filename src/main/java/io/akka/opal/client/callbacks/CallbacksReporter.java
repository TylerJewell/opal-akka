package io.akka.opal.client.callbacks;

import io.akka.opal.client.data.DataFetcher;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Telling the registered callbacks what happened — SPEC-002 R54.
 *
 * <p>A failure to reach one callback is logged and the rest still run: a report is a courtesy to
 * an interested third party, and a third party that is down must not stop the client updating its
 * own store.
 */
public final class CallbacksReporter {

  private static final Logger log = LoggerFactory.getLogger(CallbacksReporter.class);

  private final CallbacksRegister register;
  private final DataFetcher fetcher;

  private volatile Function<Data.DataUpdateReport, java.util.Map<String, Object>> userDataHandler;

  public CallbacksReporter(CallbacksRegister register, DataFetcher fetcher) {
    this.register = register;
    this.fetcher = fetcher;
  }

  public void setUserDataHandler(
      Function<Data.DataUpdateReport, java.util.Map<String, Object>> handler) {
    if (userDataHandler != null) {
      log.warn("set_user_data_handler called and already have a handler.");
    }
    this.userDataHandler = handler;
  }

  public void reportUpdateResults(
      Data.DataUpdateReport report, List<CallbacksRegister.CallbackConfig> extraCallbacks) {
    try {
      Data.DataUpdateReport toSend = report;
      if (userDataHandler != null) {
        toSend =
            new Data.DataUpdateReport(
                report.update_id(),
                report.reports(),
                report.policy_hash(),
                userDataHandler.apply(report));
      }
      String reportData = Rpc.MAPPER.writeValueAsString(toSend);

      List<CallbacksRegister.CallbackConfig> requests = new ArrayList<>();
      for (Data.CallbackEntry entry : register.all()) {
        Data.HttpFetcherConfig config =
            entry.config() == null ? Data.HttpFetcherConfig.defaults() : entry.config();
        requests.add(new CallbacksRegister.CallbackConfig(entry.url(), withData(config, reportData)));
      }
      if (extraCallbacks != null) {
        for (CallbacksRegister.CallbackConfig extra : extraCallbacks) {
          requests.add(
              new CallbacksRegister.CallbackConfig(
                  extra.url(), withData(extra.config(), reportData)));
        }
      }

      List<String> urls = new ArrayList<>();
      requests.forEach(request -> urls.add(io.akka.opal.common.util.Urls.redactUrl(request.url())));
      log.info("Reporting the update to requested callbacks {}", urls);

      // R298: every callback is sent at once, not one after another. The source gathers them,
      // so a callback that hangs delays only itself; sending them in a loop would put every
      // later callback behind the slowest one.
      List<java.util.concurrent.CompletableFuture<Void>> sent = new ArrayList<>();
      for (CallbacksRegister.CallbackConfig request : requests) {
        sent.add(
            java.util.concurrent.CompletableFuture.runAsync(
                () -> {
                  try {
                    fetcher.fetch(request.url(), request.config());
                  } catch (Exception e) {
                    log.error(
                        "Failed to send report to {}, info={}",
                        io.akka.opal.common.util.Urls.redactUrl(request.url()),
                        io.akka.opal.common.util.Urls.redactUrlInText(
                            e.toString(), request.url()));
                  }
                },
                CALLBACKS));
      }
      java.util.concurrent.CompletableFuture.allOf(sent.toArray(new java.util.concurrent.CompletableFuture[0]))
          .join();
    } catch (Exception e) {
      log.error("Failed to execute report_update_results: {}", e.toString());
    }
  }

  /** Where the callbacks are sent from, so one slow endpoint does not hold up the others. */
  private static final java.util.concurrent.ExecutorService CALLBACKS =
      java.util.concurrent.Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "opal-callbacks");
            thread.setDaemon(true);
            return thread;
          });

  private static Data.HttpFetcherConfig withData(Data.HttpFetcherConfig config, String data) {
    Data.HttpFetcherConfig base = config == null ? Data.HttpFetcherConfig.defaults() : config;
    return new Data.HttpFetcherConfig(
        base.fetcher(), base.headers(), base.is_json(), base.process_data(), base.method(), data);
  }
}
