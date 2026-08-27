package io.akka.opal;

import io.akka.opal.common.config.Enums.OpalRole;

/**
 * Which of OPAL's two processes this deployment is — SPEC-002 OD-10.
 *
 * <p>OPAL ships {@code opal-server} and {@code opal-client} as separate distributions. One
 * deployable carries both here, selected by {@code OPAL_ROLE}. A route belonging to a role this
 * deployment is not answers 404 with the same body an unmounted path answers in the original, so
 * the route set a caller can observe is exactly the one the corresponding process has.
 */
public final class Role {

  private Role() {}

  private static volatile OpalRole current;

  /**
   * Which role this process is, read once.
   *
   * <p>Once, because the source reads its whole configuration at import time and a deployment's
   * role is not something that changes while it runs. Reading the environment on every call
   * would make the route set a property of the moment a request arrived rather than of the
   * process, which is a difference nothing would report and a caller would see as routes
   * appearing and disappearing.
   */
  public static OpalRole current() {
    OpalRole answer = current;
    if (answer == null) {
      synchronized (Role.class) {
        if (current == null) {
          current = read();
        }
        answer = current;
      }
    }
    return answer;
  }

  /**
   * Re-reads {@code OPAL_ROLE}.
   *
   * <p>A deployment's role is fixed for as long as it runs, so nothing in the service calls
   * this. A harness that runs both roles one after another inside one process does.
   */
  public static void refresh() {
    synchronized (Role.class) {
      current = read();
    }
  }

  private static OpalRole read() {
    String value = System.getProperty("OPAL_ROLE", System.getenv("OPAL_ROLE"));
    if (value == null || value.isBlank()) {
      return OpalRole.both;
    }
    try {
      return OpalRole.from(value);
    } catch (IllegalArgumentException e) {
      return OpalRole.both;
    }
  }

  public static boolean isServer() {
    OpalRole role = current();
    return role == OpalRole.server || role == OpalRole.both;
  }

  public static boolean isClient() {
    OpalRole role = current();
    return role == OpalRole.client || role == OpalRole.both;
  }
}
