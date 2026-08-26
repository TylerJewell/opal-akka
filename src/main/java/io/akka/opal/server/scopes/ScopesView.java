package io.akka.opal.server.scopes;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.DeleteHandler;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.annotations.Table;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.opal.common.schemas.Scopes;
import java.util.List;

/**
 * Every scope, for {@code GET /scopes} — SPEC-002 R104.
 *
 * <p>The row is its own type rather than the entity's state: the scope's JSON travels as one text
 * column, because a scope's policy source is a discriminated union and a view row is a flat
 * record. Reading it back is one parse, and it keeps the union's shape exactly as it was written.
 */
@Component(id = "scopes-view")
public class ScopesView extends View {

  public record ScopeEntry(String scopeId, String scopeJson) {}

  public record ScopeEntries(List<ScopeEntry> scopes) {}

  @Table("scopes_view")
  @Consume.FromKeyValueEntity(ScopeEntity.class)
  public static class Updater extends TableUpdater<ScopeEntry> {

    public Effect<ScopeEntry> onUpdate(ScopeEntity.State state) {
      String scopeId = updateContext().eventSubject().orElse("");
      if (state == null || !state.exists()) {
        return effects().deleteRow();
      }
      return effects().updateRow(new ScopeEntry(scopeId, write(state.scope())));
    }

    /** A delete of the entity removes the row, so a deleted scope stops being listed. */
    @DeleteHandler
    public Effect<ScopeEntry> onDelete() {
      return effects().deleteRow();
    }

    private static String write(Scopes.Scope scope) {
      try {
        return io.akka.opal.server.pubsub.Rpc.MAPPER.writeValueAsString(scope);
      } catch (Exception e) {
        throw new IllegalStateException("could not write a scope", e);
      }
    }
  }

  @Query("SELECT * AS scopes FROM scopes_view ORDER BY scopeId")
  public QueryEffect<ScopeEntries> allScopes() {
    return queryResult();
  }
}
