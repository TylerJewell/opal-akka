package io.akka.opal.server.pubsub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

/**
 * The wire protocol OPAL's pub/sub channel speaks — SPEC-002 R58.
 *
 * <p>One JSON object per frame, carrying either a request or a response. This is
 * {@code fastapi_websocket_rpc}'s own shape rather than something OPAL invented, so a real
 * OPAL client can connect to this server and a real OPAL server can serve this client.
 */
public final class Rpc {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private Rpc() {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record RpcRequest(String method, Map<String, Object> arguments, String call_id) {
    public RpcRequest {
      if (arguments == null) {
        arguments = Map.of();
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record RpcResponse(Object result, String result_type, String call_id) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record RpcMessage(RpcRequest request, RpcResponse response) {
    public static RpcMessage request(String method, Map<String, Object> arguments, String callId) {
      return new RpcMessage(new RpcRequest(method, arguments, callId), null);
    }

    public static RpcMessage response(Object result, String resultType, String callId) {
      return new RpcMessage(null, new RpcResponse(result, resultType, callId));
    }
  }

  /** The topic every subscriber of it receives every publication on — R62. */
  public static final String ALL_TOPICS = "__EventNotifier_ALL_TOPICS__";

  /** R59: the two underscore-prefixed methods a peer may still call. */
  public static final java.util.List<String> EXPOSED_BUILT_IN_METHODS =
      java.util.List.of("_ping_", "_get_channel_id_");

  public static final String PING_RESPONSE = "pong";

  public static RpcMessage parse(String frame) throws Exception {
    return MAPPER.readValue(frame, RpcMessage.class);
  }

  public static String serialize(RpcMessage message) {
    try {
      return MAPPER.writeValueAsString(message);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static JsonNode tree(Object value) {
    return MAPPER.valueToTree(value);
  }
}
