package io.akka.opal.client.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.opal.common.schemas.Store;
import io.akka.opal.common.util.JsonPatch;
import java.util.ArrayList;
import java.util.List;

/**
 * A copy of the static data written into the engine, kept so offline mode can back it up without
 * querying — a query would also return the engine's virtual documents, which are computed rather
 * than stored and cannot be restored.
 */
public final class OpaStaticDataCache {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ObjectNode rootData = MAPPER.createObjectNode();

  public synchronized void set(String path, JsonNode data) {
    if (path == null || path.isEmpty() || path.equals("/")) {
      if (data == null || !data.isObject()) {
        throw new IllegalArgumentException("Setting root document must be a dict");
      }
      rootData = data.deepCopy();
      return;
    }
    ObjectNode parent = rootData;
    List<String> segments = segments(path);
    for (int i = 0; i < segments.size() - 1; i++) {
      JsonNode next = parent.get(segments.get(i));
      if (next instanceof ObjectNode object) {
        parent = object;
      } else {
        parent = parent.putObject(segments.get(i));
      }
    }
    parent.set(segments.get(segments.size() - 1), data);
  }

  /** Each action's path is rebased under the destination before the patch is applied. */
  public synchronized void patch(String path, List<Store.JSONPatchAction> actions) {
    ArrayNode document = MAPPER.createArrayNode();
    for (Store.JSONPatchAction action : actions) {
      ObjectNode node = MAPPER.createObjectNode();
      node.put("op", action.op());
      node.put("path", path.equals("/") ? action.path() : path + action.path());
      if (action.value() != null) {
        node.set("value", MAPPER.valueToTree(action.value()));
      }
      if (action.from() != null) {
        node.put("from", action.from());
      }
      document.add(node);
    }
    JsonNode result = JsonPatch.apply(rootData, document);
    rootData = result instanceof ObjectNode object ? object : MAPPER.createObjectNode();
  }

  public synchronized void delete(String path) {
    if (path == null || path.isEmpty() || path.equals("/")) {
      rootData = MAPPER.createObjectNode();
      return;
    }
    List<String> segments = segments(path);
    ObjectNode parent = rootData;
    for (int i = 0; i < segments.size() - 1; i++) {
      JsonNode next = parent.get(segments.get(i));
      if (!(next instanceof ObjectNode object)) {
        return;
      }
      parent = object;
    }
    parent.remove(segments.get(segments.size() - 1));
  }

  public synchronized JsonNode getData() {
    return rootData.deepCopy();
  }

  private static List<String> segments(String path) {
    List<String> out = new ArrayList<>();
    for (String segment : path.split("/")) {
      if (!segment.isEmpty()) {
        out.add(segment);
      }
    }
    if (out.isEmpty()) {
      out.add("");
    }
    return out;
  }
}
