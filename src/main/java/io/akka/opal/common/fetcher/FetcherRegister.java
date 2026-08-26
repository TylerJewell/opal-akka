package io.akka.opal.common.fetcher;

import java.lang.reflect.Constructor;
import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The providers a fetch may name — SPEC-002 R148.
 *
 * <p>A register rather than a switch, because the whole point of naming the provider in the event
 * is that a deployment can add one. The source loads them by importing the modules
 * {@code FETCH_PROVIDER_MODULES} names; here a deployment registers a factory, which is the same
 * arrangement without a language that can import a class by string at run time.
 */
public final class FetcherRegister {

  private static final Logger log = LoggerFactory.getLogger(FetcherRegister.class);

  /** Raised when the register holds nothing under the name an event asked for. */
  public static final class NoMatchingFetchProviderException extends RuntimeException {
    public NoMatchingFetchProviderException(String message) {
      super(message);
    }
  }

  /** Builds a provider for one event. */
  public interface Factory {
    FetchProvider create(FetchEvent event);
  }

  private final Map<String, Factory> providers = new LinkedHashMap<>();

  public FetcherRegister(HttpClient http, double httpTimeoutSeconds) {
    this(http, httpTimeoutSeconds, DEFAULT_PROVIDER_MODULES);
  }

  /**
   * R183: the providers named by {@code FETCH_PROVIDER_MODULES}, and nothing else.
   *
   * <p>The source imports each module the entry names and registers every fetch provider it
   * finds there, under the provider's own class name. A module it cannot load is logged and
   * skipped rather than failing start-up, because one deployment's custom provider should not
   * stop a fleet. The same arrangement here: each name is a package or a class on this
   * classpath, and a provider is anything implementing the provider interface with a
   * constructor that takes an event.
   *
   * <p>Both built-in providers live in this package, so the shipped default finds both — which
   * is what makes an entry naming the RPC provider work without any configuration.
   */
  public FetcherRegister(HttpClient http, double httpTimeoutSeconds, List<String> modules) {
    for (String module : modules == null ? DEFAULT_PROVIDER_MODULES : modules) {
      try {
        load(module, http, httpTimeoutSeconds);
      } catch (RuntimeException e) {
        log.error("Failed to load FetchingProvider module: {}", module, e);
      }
    }
    log.info("Fetcher Register loaded: {}", providers.keySet());
  }

  /** Where the two providers OPAL ships are, as the shipped configuration names them. */
  public static final List<String> DEFAULT_PROVIDER_MODULES =
      List.of("opal_common.fetcher.providers");

  private void load(String module, HttpClient http, double httpTimeoutSeconds) {
    if (BUILT_IN_MODULES.contains(module)) {
      providers.put(
          FetchEvent.DEFAULT_FETCHER,
          event -> new HttpFetchProvider(event, http, httpTimeoutSeconds));
      providers.put(RpcFetchProvider.NAME, RpcFetchProvider::new);
      return;
    }
    Class<?> found;
    try {
      found = Class.forName(module);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("no fetch provider module named " + module, e);
    }
    if (!FetchProvider.class.isAssignableFrom(found)) {
      throw new IllegalArgumentException(module + " is not a fetch provider");
    }
    Constructor<?> constructor;
    try {
      constructor = found.getConstructor(FetchEvent.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          module + " has no constructor taking a fetch event", e);
    }
    log.info("Loading FetcherProvider '{}' found at: {}", found.getSimpleName(), module);
    providers.put(
        found.getSimpleName(),
        event -> {
          try {
            return (FetchProvider) constructor.newInstance(event);
          } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("could not build " + module, e);
          }
        });
  }

  /**
   * The names that mean "the providers OPAL ships".
   *
   * <p>The first is the source's own module path, which is what the shipped default holds and
   * therefore what a deployment that has not changed anything asks for; the second is where the
   * same two providers live here. Both answer, so an unchanged deployment gets both providers and
   * one written against this rebuild can say so in its own terms.
   */
  private static final java.util.Set<String> BUILT_IN_MODULES =
      java.util.Set.of("opal_common.fetcher.providers", "io.akka.opal.common.fetcher");

  public Set<String> names() {
    return providers.keySet();
  }

  public void registerFetcher(String name, Factory factory) {
    providers.put(name, factory);
  }

  public FetchProvider getFetcher(String name, FetchEvent event) {
    Factory factory = providers.get(name);
    if (factory == null) {
      throw new NoMatchingFetchProviderException(
          "Couldn't find a match for - " + name + " , " + event);
    }
    return factory.create(event);
  }

  public FetchProvider getFetcherForEvent(FetchEvent event) {
    return getFetcher(event.fetcher(), event);
  }
}
