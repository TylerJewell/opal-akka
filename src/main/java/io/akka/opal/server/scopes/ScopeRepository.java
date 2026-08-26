package io.akka.opal.server.scopes;

import akka.javasdk.client.ComponentClient;
import io.akka.opal.common.schemas.Scopes;
import io.akka.opal.server.pubsub.Rpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reading and writing scopes through the entity that holds them.
 *
 * <p>A read of one scope goes to the entity, and a read of all of them goes to the view. The two
 * are not the same freshness: the view is a projection and lags a write by the time it takes to
 * be applied, which is why {@code PUT} then {@code GET /scopes/{id}} is consistent and
 * {@code PUT} then {@code GET /scopes} may not be.
 */
public final class ScopeRepository {

  /** Raised when a scope id names nothing. */
  public static final class ScopeNotFound extends RuntimeException {
    public ScopeNotFound(String scopeId) {
      super("No such scope: " + scopeId);
    }
  }

  private final ComponentClient componentClient;

  public ScopeRepository(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public Scopes.Scope get(String scopeId) {
    ScopeEntity.State state =
        componentClient
            .forKeyValueEntity(scopeId)
            .method(ScopeEntity::get)
            .invoke();
    if (state == null || !state.exists()) {
      throw new ScopeNotFound(scopeId);
    }
    return state.scope();
  }

  public Optional<Scopes.Scope> find(String scopeId) {
    try {
      return Optional.of(get(scopeId));
    } catch (ScopeNotFound e) {
      return Optional.empty();
    }
  }

  public void put(Scopes.Scope scope) {
    componentClient
        .forKeyValueEntity(scope.scope_id())
        .method(ScopeEntity::put)
        .invoke(scope);
  }

  public void delete(String scopeId) {
    componentClient.forKeyValueEntity(scopeId).method(ScopeEntity::delete).invoke();
  }

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(ScopeRepository.class);

  public List<Scopes.Scope> all() {
    ScopesView.ScopeEntries rows =
        componentClient.forView().method(ScopesView::allScopes).invoke();
    List<Scopes.Scope> scopes = new ArrayList<>();
    if (rows == null || rows.scopes() == null) {
      return scopes;
    }
    for (ScopesView.ScopeEntry row : rows.scopes()) {
      // R245: a row this cannot read is skipped rather than failing the listing. Every pass over
      // the scopes — the periodic sync, the purge's sibling check, the listing route — reads this,
      // and one unreadable row would otherwise stop all of them for every other tenant.
      if (row.scopeJson() == null || row.scopeJson().isEmpty()) {
        continue;
      }
      try {
        scopes.add(Rpc.MAPPER.readValue(row.scopeJson(), Scopes.Scope.class));
      } catch (Exception e) {
        log.warn("skipping scope {}, which could not be read: {}", row.scopeId(), e.toString());
      }
    }
    return scopes;
  }
}
