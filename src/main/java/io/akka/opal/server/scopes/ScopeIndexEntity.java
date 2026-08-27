package io.akka.opal.server.scopes;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Which scopes exist — SPEC-002 R400.
 *
 * <p>OPAL lists its scopes by asking Redis for every key under one prefix, and gets the answer as
 * of the moment it asks. The listing is not only a convenience: a purge decides whether to remove
 * a clone by asking which scopes still name that source, and an answer that lags a write by even
 * a little can say "nobody" about a scope somebody has just created against the same repository.
 *
 * <p>A projection over the scope entities answers that question late for the same reason it
 * answers a dashboard cheaply. So the set of ids lives here, in an entity of its own, written
 * before the scope it names and read straight back — and the scopes themselves are then read one
 * by one from the entities that hold them, which is where the current value is.
 */
@Component(id = "scope-index")
public class ScopeIndexEntity extends KeyValueEntity<ScopeIndexEntity.State> {

  /**
   * How many scopes this can hold, and why there is a number at all.
   *
   * <p>OPAL's index is Redis's own keyspace, which has no such bound. Here it is one entity's
   * state, and this runtime replicates a state of up to a megabyte — which is about twenty-five
   * thousand ids of the length a tenant name has. A deployment past that needs the listing split
   * across several index entities, and this is where a reader looking for the limit will come.
   */
  public static final int ROUGH_CEILING = 25_000;

  /** The one identity this entity ever has: there is a single index. */
  public static final String ID = "scopes";

  public record State(List<String> scopeIds) {
    public State {
      if (scopeIds == null) {
        scopeIds = List.of();
      }
    }
  }

  @Override
  public State emptyState() {
    return new State(List.of());
  }

  public Effect<Done> add(String scopeId) {
    Set<String> ids = new LinkedHashSet<>(currentState().scopeIds());
    if (!ids.add(scopeId)) {
      return effects().reply(Done.getInstance());
    }
    return effects().updateState(new State(List.copyOf(ids))).thenReply(Done.getInstance());
  }

  public Effect<Done> remove(String scopeId) {
    Set<String> ids = new LinkedHashSet<>(currentState().scopeIds());
    if (!ids.remove(scopeId)) {
      return effects().reply(Done.getInstance());
    }
    return effects().updateState(new State(List.copyOf(ids))).thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<State> get() {
    return effects().reply(currentState());
  }
}
