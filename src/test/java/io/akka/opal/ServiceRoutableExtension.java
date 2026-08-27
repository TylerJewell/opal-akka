package io.akka.opal;

import akka.javasdk.http.StrictResponse;
import akka.javasdk.testkit.TestKitSupport;
import akka.util.ByteString;
import java.lang.reflect.Field;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Waits for the service's routes to answer before the first test in a class runs.
 *
 * <p>The test kit decides the runtime has started when the runtime's own health path answers
 * {@code 404} — which it does as soon as the port is open, and which is the same answer a route
 * that has not been registered yet gives. On a loaded machine the registration lands after the
 * first tests have run, and a whole class then fails with every route reporting {@code 404}: the
 * one status that means both "the service does not have this route" and "the service is not
 * listening for it yet".
 *
 * <p>Registered by auto-detection, so every integration class gets it without saying so. A class
 * that is not a test kit class is left alone.
 */
public final class ServiceRoutableExtension implements BeforeEachCallback {

  private static final String PROBE = "/docs";

  private static final long BUDGET_MILLIS = 30_000;

  @Override
  public void beforeEach(ExtensionContext context) {
    Object instance = context.getTestInstance().orElse(null);
    if (!(instance instanceof TestKitSupport support)) {
      return;
    }
    ExtensionContext.Store store =
        context.getStore(ExtensionContext.Namespace.create(ServiceRoutableExtension.class));
    if (store.get(context.getRequiredTestClass()) != null) {
      return;
    }
    store.put(context.getRequiredTestClass(), Boolean.TRUE);
    awaitRoutable(support);
  }

  private void awaitRoutable(TestKitSupport support) {
    akka.javasdk.http.HttpClient client = httpClientOf(support);
    if (client == null) {
      return;
    }
    long deadline = System.nanoTime() + BUDGET_MILLIS * 1_000_000L;
    while (System.nanoTime() < deadline) {
      try {
        StrictResponse<ByteString> response = client.GET(PROBE).invoke();
        if (response.status().intValue() != 404) {
          return;
        }
      } catch (RuntimeException e) {
        // Not answering at all yet, which is the same wait.
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static akka.javasdk.http.HttpClient httpClientOf(TestKitSupport support) {
    for (Class<?> type = support.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField("httpClient");
        field.setAccessible(true);
        return (akka.javasdk.http.HttpClient) field.get(support);
      } catch (ReflectiveOperationException | RuntimeException e) {
        // Keep looking up the hierarchy; a kit without the field needs no wait.
      }
    }
    return null;
  }
}
