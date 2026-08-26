package io.akka.opal;

import akka.javasdk.DependencyProvider;
import akka.javasdk.ServiceSetup;
import akka.javasdk.annotations.Setup;
import akka.javasdk.client.ComponentClient;
import io.akka.opal.client.ClientRuntime;
import io.akka.opal.common.config.CommonConfig;
import io.akka.opal.client.config.ClientConfig;
import io.akka.opal.server.ServerRuntime;
import io.akka.opal.server.config.ServerConfig;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the configuration, builds whichever of the two runtimes this deployment's role names, and
 * hands them to the endpoints.
 *
 * <p>The configuration is read once, here. OPAL evaluates its entries at import time and every
 * module reaches for a global; the same values are read the same way, in the same declaration
 * order, and then passed rather than reached for — so a test can build a configuration from a map
 * instead of from the process environment.
 */
@Setup
public class Bootstrap implements ServiceSetup {

  private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

  private final ComponentClient componentClient;

  private volatile CommonConfig common;
  private volatile ServerRuntime serverRuntime;
  private volatile ClientRuntime clientRuntime;
  private boolean built;

  public Bootstrap(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  /**
   * The environment, overlaid with any {@code OPAL_}-prefixed system property.
   *
   * <p>OPAL reads its entries from environment variables, from the command line and from
   * {@code .env}/{@code .ini} files — three ways of saying the same thing. This adds a fourth
   * that the JVM already has, so a deployment can pass {@code -DOPAL_SCOPES=true} the way it
   * passes every other JVM setting, and a test can configure a service it does not fork.
   */
  static Map<String, String> environment() {
    Map<String, String> environment = new HashMap<>(System.getenv());
    System.getProperties()
        .forEach(
            (key, value) -> {
              String name = String.valueOf(key);
              if (name.startsWith("OPAL_")) {
                environment.put(name, String.valueOf(value));
              }
            });
    return environment;
  }

  /**
   * Reads the configuration and builds the runtimes the current role names.
   *
   * <p>Called from both {@link #onStartup()} and {@link #createDependencyProvider()}'s provider,
   * because a request can reach an endpoint before the start-up hook has run: the hook is one of
   * several things the runtime starts, and the HTTP port is another. An endpoint whose dependency
   * is still null answers 500, so the first caller builds what it needs and the hook finds it
   * already there.
   */
  private synchronized void build() {
    if (built) {
      return;
    }
    built = true;
    Map<String, String> environment = environment();
    common = new CommonConfig(environment);
    io.akka.opal.common.logging.Logs.configure(common);
    io.akka.opal.common.util.Http.configureClientTrust(
        Boolean.TRUE.equals(common.get("CLIENT_SELF_SIGNED_CERTIFICATES_ALLOWED")),
        common.getString("CLIENT_SSL_CONTEXT_TRUSTED_CA_FILE"));
    io.akka.opal.common.git.RepoCloner.configureSshKeyFile(
        common.getString("GIT_SSH_KEY_FILE"));
    io.akka.opal.api.Responses.configureCors(common.get("ALLOWED_ORIGINS"));
    log.info("OPAL role: {}", Role.current().wire());
    if (Role.isServer()) {
      ServerConfig serverConfig = new ServerConfig(environment);
      serverConfig.onLoad();
      serverRuntime = new ServerRuntime(common, serverConfig, componentClient);
    }
    if (Role.isClient()) {
      ClientConfig clientConfig = new ClientConfig(environment);
      clientConfig.onLoad(common);
      clientRuntime = new ClientRuntime(common, clientConfig);
    }
  }

  @Override
  public void onStartup() {
    build();
    if (serverRuntime != null) {
      serverRuntime.startPolicySource();
    }
    if (clientRuntime != null) {
      // The client's first act is to reach the server, and under `both` that server is this
      // process — so it starts on its own thread rather than holding up start-up.
      Thread starter =
          new Thread(
              () -> {
                try {
                  clientRuntime.start();
                } catch (Exception e) {
                  log.error("could not start the OPAL client", e);
                }
              },
              "opal-client-start");
      starter.setDaemon(true);
      starter.start();
    }
  }

  @Override
  public void onShutdown() {
    if (clientRuntime != null) {
      clientRuntime.close();
    }
    if (serverRuntime != null) {
      serverRuntime.shutdown();
    }
  }

  @Override
  public DependencyProvider createDependencyProvider() {
    return new DependencyProvider() {
      @Override
      @SuppressWarnings("unchecked")
      public <T> T getDependency(Class<T> clazz) {
        build();
        if (clazz == ServerRuntime.class) {
          return (T) serverRuntime;
        }
        if (clazz == ClientRuntime.class) {
          return (T) clientRuntime;
        }
        if (clazz == CommonConfig.class) {
          return (T) common;
        }
        throw new IllegalArgumentException("no such dependency: " + clazz.getName());
      }
    };
  }
}
