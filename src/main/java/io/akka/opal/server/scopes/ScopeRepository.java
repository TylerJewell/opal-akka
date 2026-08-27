package io.akka.opal.server.scopes;

import akka.javasdk.client.ComponentClient;
import io.akka.opal.common.schemas.Scopes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reading and writing scopes through the entities that hold them.
 *
 * <p>R400: every read goes to an entity, one scope at a time, with the set of ids read from an
 * index entity written before the scope it names. A projection would answer the listing more
 * cheaply and would answer it late, and the listing is what a purge's sibling check reads before
 * deciding whether a clone is still wanted — so {@code PUT} then {@code GET /scopes} shows the
 * new scope, and a delete published a moment after a sibling was created does not remove that
 * sibling's clone.
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
    // The id is registered first: an id in the index whose scope is not there yet is skipped by
    // the listing, and a scope not in the index is invisible to the sibling check that decides
    // whether its clone survives somebody else's delete. Only the first of those is harmless.
    componentClient
        .forKeyValueEntity(ScopeIndexEntity.ID)
        .method(ScopeIndexEntity::add)
        .invoke(scope.scope_id());
    componentClient
        .forKeyValueEntity(scope.scope_id())
        .method(ScopeEntity::put)
        .invoke(scope);
  }

  public void delete(String scopeId) {
    componentClient.forKeyValueEntity(scopeId).method(ScopeEntity::delete).invoke();
    componentClient
        .forKeyValueEntity(ScopeIndexEntity.ID)
        .method(ScopeIndexEntity::remove)
        .invoke(scopeId);
  }

  public List<Scopes.Scope> all() {
    ScopeIndexEntity.State index =
        componentClient
            .forKeyValueEntity(ScopeIndexEntity.ID)
            .method(ScopeIndexEntity::get)
            .invoke();
    List<Scopes.Scope> scopes = new ArrayList<>();
    if (index == null) {
      return scopes;
    }
    for (String scopeId : index.scopeIds()) {
      // R245: an id whose scope is not there is one deleted between this listing and the read,
      // or one registered by a write that has not landed yet, and neither is a scope. A scope
      // that IS there and does not parse is corruption, and is raised — dropping it silently
      // takes the scope out of the listing, out of the sync, and out of the sibling check a
      // delete's purge reads, which is what decides whether a live tenant's clone is removed.
      ScopeEntity.State state =
          componentClient.forKeyValueEntity(scopeId).method(ScopeEntity::get).invoke();
      if (state == null || !state.exists()) {
        continue;
      }
      scopes.add(state.scope());
    }
    return scopes;
  }
}
