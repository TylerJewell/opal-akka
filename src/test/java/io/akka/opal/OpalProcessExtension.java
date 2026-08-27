package io.akka.opal;

import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Runs a test class's {@code startProcess()} at the moment that class starts.
 *
 * <p>OPAL's configuration is process-wide and its role is read once, so a test class that wants a
 * different role or a different policy store is describing a different process. That environment
 * belongs here rather than in a static initializer: a static initializer runs when its class is
 * loaded, which happens for every class the runner discovers, not only for the one about to
 * execute — so one class's initializer decides another class's environment and discovery order
 * picks the winner. A class carrying this extension declares
 *
 * <pre>{@code static void startProcess() { ... } }</pre>
 *
 * and it is called once, before that class's first test, with the role re-read afterwards.
 */
public final class OpalProcessExtension implements BeforeAllCallback, AfterAllCallback {

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    Class<?> testClass = context.getRequiredTestClass();
    Method start = null;
    for (Method candidate : testClass.getDeclaredMethods()) {
      if (candidate.getName().equals("startProcess") && candidate.getParameterCount() == 0) {
        start = candidate;
        break;
      }
    }
    if (start == null) {
      throw new IllegalStateException(
          testClass.getName() + " carries OpalProcessExtension and declares no startProcess()");
    }
    start.setAccessible(true);
    start.invoke(null);
    Role.refresh();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    Role.refresh();
  }
}
