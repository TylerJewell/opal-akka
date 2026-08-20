package io.akka.opal.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.StepName;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.workflow.Workflow;
import akka.javasdk.workflow.WorkflowContext;
import io.akka.opal.domain.Change;
import io.akka.opal.domain.Outcome;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Getting one change to everyone it is addressed to — SPEC-001 §3 rules 3, 15, 19.
 *
 * <p>One workflow per change, named after the change, so the fan-out survives a restart and
 * the same change is never fanned out twice. The source has no equivalent: it publishes onto
 * a socket and the delivery is over (question-log row 22), which is why an unreachable
 * subscriber there needs a replay buffer and a refetch to recover (rows 18, 19).
 *
 * <p>A subscriber that does not end up level is handed to {@link CatchUpAction} on a timer
 * rather than retried here. Retrying the same delivery would hand it the same change again,
 * and a change above the next expected number is refused however many times it arrives — what
 * that subscriber needs is the span it is missing, which is a different request.
 */
@Component(id = "fan-out")
public class FanOutWorkflow extends Workflow<FanOutWorkflow.FanOut> {

  private static final Logger logger = LoggerFactory.getLogger(FanOutWorkflow.class);

  /** How long a subscriber left behind is given before it is caught up. */
  static final Duration CATCH_UP_DELAY = Duration.ofSeconds(3);

  /**
   * @param targets everyone the change is addressed to
   * @param applied those now level with this change
   * @param behind those that are not, for whatever reason
   */
  public record FanOut(Change change, List<String> targets, List<String> applied,
      List<String> behind) {

    FanOut withTargets(List<String> found) {
      return new FanOut(change, found, applied, behind);
    }

    FanOut withOutcomes(List<String> nowApplied, List<String> nowBehind) {
      return new FanOut(change, targets, nowApplied, nowBehind);
    }
  }

  private final ComponentClient componentClient;
  private final String changeId;

  public FanOutWorkflow(ComponentClient componentClient, WorkflowContext context) {
    this.componentClient = componentClient;
    this.changeId = context.workflowId();
  }

  @Override
  public WorkflowSettings settings() {
    // The whole-workflow timeout is a safety net, not a working limit: a fan-out that has
    // not finished in a minute is stuck, and a stuck one holds a catch-up timer that will
    // never be armed.
    return WorkflowSettings.builder()
        .defaultStepTimeout(Duration.ofSeconds(10))
        .timeout(Duration.ofMinutes(1))
        .build();
  }

  /**
   * Idempotent on purpose: the consumer that calls this is delivered at least once, so the
   * same change can arrive twice. A second start on a workflow that already has state would
   * fan the change out again — harmless at each cursor, which refuses a repeat, but it would
   * also re-arm every catch-up timer.
   */
  public Effect<Done> start(Change change) {
    if (currentState() != null) {
      return effects().reply(Done.getInstance());
    }
    return effects()
        .updateState(new FanOut(change, List.of(), List.of(), List.of()))
        .transitionTo(FanOutWorkflow::findTargetsStep)
        .thenReply(Done.getInstance());
  }

  public ReadOnlyEffect<FanOut> read() {
    return effects().reply(currentState());
  }

  @StepName("find-targets")
  private StepEffect findTargetsStep() {
    Set<String> targets = new LinkedHashSet<>();
    for (String topic : currentState().change().topics()) {
      SubscribersByTopicView.Subscribers found =
          componentClient.forView().method(SubscribersByTopicView::byTopic).invoke(topic);
      found.items().forEach(subscription -> targets.add(subscription.subscriber()));
    }
    return stepEffects()
        .updateState(currentState().withTargets(List.copyOf(targets)))
        .thenTransitionTo(FanOutWorkflow::deliverStep);
  }

  @StepName("deliver")
  private StepEffect deliverStep() {
    Change change = currentState().change();
    List<String> applied = new ArrayList<>();
    List<String> behind = new ArrayList<>();

    for (String subscriber : currentState().targets()) {
      CursorEntity.Result result =
          componentClient
              .forEventSourcedEntity(CursorEntity.Id.of(subscriber, change.destination()))
              .method(CursorEntity::apply)
              .invoke(new CursorEntity.Apply(change.sequence(), change.payload()));
      if (result.outcome() == Outcome.GAP) {
        behind.add(subscriber);
      } else {
        applied.add(subscriber);
      }
    }

    return stepEffects()
        .updateState(currentState().withOutcomes(applied, behind))
        .thenTransitionTo(FanOutWorkflow::settleStep);
  }

  @StepName("settle")
  private StepEffect settleStep() {
    Change change = currentState().change();
    for (String subscriber : currentState().behind()) {
      logger.info(
          "{} is behind on {}, catching up after {}",
          subscriber,
          change.destination(),
          CATCH_UP_DELAY);
      timers()
          .createSingleTimer(
              "catch-up-" + changeId + "-" + subscriber,
              CATCH_UP_DELAY,
              componentClient
                  .forTimedAction()
                  .method(CatchUpAction::catchUp)
                  .deferred(new CatchUpAction.Request(subscriber, change.destination())));
    }
    return stepEffects().thenEnd();
  }
}
