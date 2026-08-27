package io.akka.opal.common.fetcher;

import com.fasterxml.jackson.databind.JsonNode;
import io.akka.opal.common.util.Http;
import io.akka.opal.common.util.Urls;
import io.akka.opal.server.pubsub.Rpc;
import java.net.URI;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The second provider OPAL ships — SPEC-002 R182.
 *
 * <p>It fetches by calling a method on a websocket RPC server rather than by asking an HTTP
 * endpoint, which is how a deployment reads data out of something that already holds an open
 * channel to it. The entry's configuration names the method and its arguments; the answer is
 * whatever the method returned.
 *
 * <p>The arguments are never logged. They are where a caller puts a credential, and the source
 * masks them in its own repr for that reason.
 */
public final class RpcFetchProvider implements FetchProvider {

  private static final Logger log = LoggerFactory.getLogger(RpcFetchProvider.class);

  /** The name an entry's `fetcher` field uses to ask for this one. */
  public static final String NAME = "FastApiRpcFetchProvider";

  /** R182: how long the source waits for an answer before giving up on the call. */
  public static final int RESPONSE_TIMEOUT_SECONDS = 4;

  private final FetchEvent event;

  public RpcFetchProvider(FetchEvent event) {
    this.event = event;
  }

  @Override
  public Object fetch() {
    Map<String, Object> config = event.config();
    Object method = config == null ? null : config.get("rpc_method_name");
    if (!(method instanceof String name) || name.isEmpty()) {
      throw new IllegalArgumentException(
          "FastApiRpcFetchProvider needs rpc_method_name in the entry's config");
    }
    Object rawArguments = config.get("rpc_arguments");
    // Required by the source's own config schema, so an entry without it is refused rather than
    // fetched with no arguments.
    if (!(rawArguments instanceof Map<?, ?> given)) {
      throw new io.akka.opal.common.schemas.Schemas.ValidationFailure(
          "rpc_arguments: field required");
    }
    Map<String, Object> arguments = castArguments(given);

    log.info(
        "RpcFetchProvider fetching from {} with RPC call {}",
        Urls.redactUrl(event.url()),
        name);

    AtomicReference<Object> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch answered = new CountDownLatch(1);
    String callId = UUID.randomUUID().toString().replace("-", "");
    StringBuilder partial = new StringBuilder();

    WebSocket socket = null;
    try {
      socket =
          Http.forClient()
              .newWebSocketBuilder()
              .connectTimeout(Duration.ofSeconds(RESPONSE_TIMEOUT_SECONDS))
              .buildAsync(
                  URI.create(event.url()),
                  new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                      webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onText(
                        WebSocket webSocket, CharSequence data, boolean last) {
                      partial.append(data);
                      if (last) {
                        String frame = partial.toString();
                        partial.setLength(0);
                        try {
                          Rpc.RpcMessage message = Rpc.parse(frame);
                          if (message.response() != null
                              && callId.equals(message.response().call_id())) {
                            result.set(message.response().result());
                            answered.countDown();
                          }
                        } catch (Exception e) {
                          failure.set(e);
                          answered.countDown();
                        }
                      }
                      webSocket.request(1);
                      return null;
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable error) {
                      failure.set(error);
                      answered.countDown();
                    }

                    @Override
                    public CompletionStage<?> onClose(
                        WebSocket webSocket, int statusCode, String reason) {
                      answered.countDown();
                      return null;
                    }
                  })
              .get(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      socket.sendText(Rpc.serialize(Rpc.RpcMessage.request(name, arguments, callId)), true);
      if (!answered.await(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out calling " + name);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted calling " + name, e);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("could not call " + name, e);
    } finally {
      if (socket != null) {
        socket.abort();
      }
    }

    if (failure.get() != null) {
      throw new IllegalStateException("RPC fetch failed", failure.get());
    }
    return result.get();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castArguments(Map<?, ?> given) {
    return (Map<String, Object>) given;
  }

  @Override
  public JsonNode process(Object raw) {
    return Rpc.tree(raw);
  }
}
