package io.akka.opal.server.scopes;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import io.akka.opal.common.schemas.Scopes;

/**
 * One scope, durably — SPEC-002 R103 and OD-3.
 *
 * <p>OPAL keeps scopes in Redis under {@code permit.io/Scope:{id}}. Here each scope is an entity
 * of the runtime this is built on, with the same identity and the same JSON on the wire; what a
 * caller sees through the scopes routes is unchanged, and the store the deployment has to run is
 * one fewer.
 */
@Component(id = "scope")
public class ScopeEntity extends KeyValueEntity<ScopeEntity.State> {

  /** Absent until a scope is put; {@code scope} is null while that is so. */
  public record State(Scopes.Scope scope) {
    public boolean exists() {
      return scope != null;
    }
  }

  @Override
  public State emptyState() {
    return new State(null);
  }

  public Effect<Done> put(Scopes.Scope scope) {
    return effects().updateState(new State(scope)).thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<State> get() {
    return effects().reply(currentState());
  }

  /**
   * Deleting a scope that was never there is not an error — R105 answers 204 either way, and a
   * caller retrying a delete it already made should get the same answer as the first time.
   */
  public Effect<Done> delete() {
    return effects().deleteEntity().thenReply(Done.getInstance());
  }
}
