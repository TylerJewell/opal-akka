package io.akka.opal.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/** The policy store's own transaction schema — SPEC-002 section 2.2. */
public final class Store {

  private Store() {}

  public enum TransactionType {
    policy,
    data
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record RemoteStatus(String remote_url, boolean succeed, String error) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record StoreTransaction(
      String id,
      List<String> actions,
      TransactionType transaction_type,
      boolean success,
      String error,
      String creation_time,
      String end_time,
      List<RemoteStatus> remotes_status) {}

  /**
   * One RFC-6902 operation — SPEC-002 R143.
   *
   * <p>{@code add} and {@code replace} are the two operations whose meaning depends on a value,
   * so an action naming either without one is refused where it is built rather than where it is
   * applied: the source rejects it at the schema and never reaches its patch library.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  public record JSONPatchAction(String op, String path, Object value, String from)
      implements io.akka.opal.common.util.Repr.Reprable {
    public JSONPatchAction {
      if (("add".equals(op) || "replace".equals(op)) && value == null) {
        throw new Schemas.ValidationFailure(
            "'value' must be present when op is either add or replace");
      }
    }

    /** The source names this field {@code from_field} and aliases it to {@code from} on the wire. */
    @Override
    public List<String> pyFields() {
      return List.of(
          "op=" + io.akka.opal.common.util.Repr.repr(op),
          "path=" + io.akka.opal.common.util.Repr.repr(path),
          "value=" + io.akka.opal.common.util.Repr.repr(value),
          "from_field=" + io.akka.opal.common.util.Repr.repr(from));
    }

    /**
     * R192: the action that adds a value to the end of an array.
     *
     * <p>The source gives this its own name because {@code "-"} as a path is the JSON Patch
     * spelling of "past the last element", which a caller writing one by hand gets wrong. It is
     * an ordinary add action underneath, which is why nothing special reads it.
     */
    public static JSONPatchAction arrayAppend(Object value) {
      return new JSONPatchAction("add", "-", value, null);
    }
  }
}
