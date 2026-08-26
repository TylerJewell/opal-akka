package io.akka.opal.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;

/**
 * RFC 6902 JSON Patch, applied in place — the six operations OPAL's own static-data cache and
 * mock store apply when a data update arrives with {@code save_method: PATCH}.
 *
 * <p>A patch is all-or-nothing in the specification, and this follows it: a failing operation
 * raises and leaves the caller to discard the document rather than keeping a half-applied one.
 */
public final class JsonPatch {

  /** Raised when an operation cannot be applied to the document it was given. */
  public static final class PatchFailed extends RuntimeException {
    public PatchFailed(String message) {
      super(message);
    }
  }

  private JsonPatch() {}

  public static JsonNode apply(JsonNode document, JsonNode patch) {
    if (!patch.isArray()) {
      throw new PatchFailed("a patch document must be an array of operations");
    }
    JsonNode result = document == null ? null : document.deepCopy();
    for (JsonNode operation : patch) {
      result = applyOne(result, operation);
    }
    return result;
  }

  private static JsonNode applyOne(JsonNode document, JsonNode operation) {
    String op = text(operation, "op");
    String path = text(operation, "path");
    switch (op) {
      case "add":
        return add(document, split(path), operation.get("value"));
      case "remove":
        return remove(document, split(path));
      case "replace": {
        JsonNode after = remove(document, split(path));
        return add(after, split(path), operation.get("value"));
      }
      case "move": {
        List<String> from = split(text(operation, "from"));
        JsonNode value = get(document, from);
        JsonNode after = remove(document, from);
        return add(after, split(path), value);
      }
      case "copy": {
        JsonNode value = get(document, split(text(operation, "from")));
        return add(document, split(path), value == null ? null : value.deepCopy());
      }
      case "test": {
        JsonNode value = get(document, split(path));
        JsonNode expected = operation.get("value");
        if (value == null ? expected != null && !expected.isNull() : !value.equals(expected)) {
          throw new PatchFailed("test operation failed at " + path);
        }
        return document;
      }
      default:
        throw new PatchFailed("unknown patch operation " + op);
    }
  }

  private static String text(JsonNode operation, String field) {
    JsonNode node = operation.get(field);
    if (node == null || !node.isTextual()) {
      throw new PatchFailed("patch operation is missing '" + field + "'");
    }
    return node.textValue();
  }

  /** JSON Pointer, with the two escapes the specification defines. */
  static List<String> split(String pointer) {
    List<String> tokens = new ArrayList<>();
    if (pointer.isEmpty()) {
      return tokens;
    }
    if (!pointer.startsWith("/")) {
      throw new PatchFailed("a JSON pointer must start with '/': " + pointer);
    }
    for (String raw : pointer.substring(1).split("/", -1)) {
      tokens.add(raw.replace("~1", "/").replace("~0", "~"));
    }
    return tokens;
  }

  public static JsonNode get(JsonNode document, List<String> tokens) {
    JsonNode current = document;
    for (String token : tokens) {
      if (current == null) {
        return null;
      }
      if (current.isArray()) {
        int index = index(token, ((ArrayNode) current).size(), false);
        current = current.get(index);
      } else if (current.isObject()) {
        current = current.get(token);
      } else {
        return null;
      }
    }
    return current;
  }

  private static JsonNode add(JsonNode document, List<String> tokens, JsonNode value) {
    if (tokens.isEmpty()) {
      return value;
    }
    JsonNode parent = get(document, tokens.subList(0, tokens.size() - 1));
    String last = tokens.get(tokens.size() - 1);
    if (parent instanceof ObjectNode object) {
      object.set(last, value);
    } else if (parent instanceof ArrayNode array) {
      int index = index(last, array.size(), true);
      array.insert(index, value);
    } else {
      throw new PatchFailed("cannot add at " + String.join("/", tokens));
    }
    return document;
  }

  private static JsonNode remove(JsonNode document, List<String> tokens) {
    if (tokens.isEmpty()) {
      return null;
    }
    JsonNode parent = get(document, tokens.subList(0, tokens.size() - 1));
    String last = tokens.get(tokens.size() - 1);
    if (parent instanceof ObjectNode object) {
      if (object.remove(last) == null) {
        throw new PatchFailed("cannot remove missing member " + last);
      }
    } else if (parent instanceof ArrayNode array) {
      int index = index(last, array.size(), false);
      array.remove(index);
    } else {
      throw new PatchFailed("cannot remove at " + String.join("/", tokens));
    }
    return document;
  }

  /** {@code -} names the position past the end, which only an add may use. */
  private static int index(String token, int size, boolean allowEnd) {
    if (token.equals("-")) {
      if (!allowEnd) {
        throw new PatchFailed("'-' is not a member of an array");
      }
      return size;
    }
    int index;
    try {
      index = Integer.parseInt(token);
    } catch (NumberFormatException e) {
      throw new PatchFailed("array index must be a number: " + token);
    }
    if (index < 0 || index > size || (!allowEnd && index == size)) {
      throw new PatchFailed("array index out of range: " + token);
    }
    return index;
  }
}
