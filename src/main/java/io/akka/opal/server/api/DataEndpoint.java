package io.akka.opal.server.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.http.AbstractHttpEndpoint;
import io.akka.opal.Role;
import io.akka.opal.api.Responses;
import io.akka.opal.common.auth.Authz;
import io.akka.opal.common.auth.Unauthorized;
import io.akka.opal.common.schemas.Data;
import io.akka.opal.common.schemas.Security.PeerType;
import io.akka.opal.server.ServerRuntime;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data routes — SPEC-002 R45, R56, R131 and R132.
 *
 * <p>{@code GET /data/config} is what a client asks on connect to find out where to read
 * everything from; {@code POST /data/config} is how a data source tells the fleet something
 * changed. They share a path and have nothing else in common.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint
public class DataEndpoint extends AbstractHttpEndpoint {

  private static final Logger log = LoggerFactory.getLogger(DataEndpoint.class);

  private final ServerRuntime runtime;

  public DataEndpoint(ServerRuntime runtime) {
    this.runtime = runtime;
  }

  /** R131: the stand-in the default data configuration points at. */
  @Get("/policy-data")
  public HttpResponse defaultAllData() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      log.info("Serving default all-data, DATA_CONFIG_SOURCES was not configured");
      return Responses.ok(Map.of());
    });
  }

  /** R56: the configured sources, or a redirect to whoever serves them per client. */
  @Get("/data/config")
  public HttpResponse getDataSourcesConfig() {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      try {
        Authn.requireLoggedIn(runtime.signer(), requestContext());
      } catch (Unauthorized e) {
        return Responses.unauthorized(e);
      }
      Data.ServerDataSourceConfig config = runtime.config().get("DATA_CONFIG_SOURCES");
      if (config == null) {
        return Responses.detail(
            StatusCodes.INTERNAL_SERVER_ERROR, "no data sources configuration was configured");
      }
      // R43: an inline configuration answers, and the redirect is only for a deployment that
      // has none. A configuration carrying both is refused by its own validator, so the order
      // matters only for one built in code rather than read from the environment — and there
      // the source serves the inline one.
      if (config.config() != null) {
        log.info("Serving source configuration");
        return Responses.ok(config.config());
      }
      if (config.external_source_url() != null) {
        String token = Authn.bearerToken(requestContext());
        String url = withQueryParam(config.external_source_url(), "token", token);
        log.info(
            "Source configuration is available at '{}', redirecting with token={} (abbrv.)",
            config.external_source_url(),
            abbreviate(token));
        return Responses.redirect(url);
      }
      log.error(
          "data source configuration is invalid: neither an inline config "
              + "nor an external_source_url was provided");
      return Responses.detail(
          StatusCodes.INTERNAL_SERVER_ERROR, "no data sources configuration was configured");
    });
  }

  /**
   * The first and last five characters of a token, which is what the source logs.
   *
   * <p>Enough to tell two tokens apart in a log and not enough to use one.
   */
  static String abbreviate(String token) {
    if (token == null || token.length() < 10) {
      return "...";
    }
    return token.substring(0, 5) + "..." + token.substring(token.length() - 5);
  }

  /** R45: a data source publishes an update, inside whatever topics its token permits. */
  @Post("/data/config")
  public HttpResponse publishDataUpdateEvent(Data.DataUpdate update) {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      // R199: a server with no publisher has nothing to publish through, and says so the way
      // the source does — by failing where it reaches for one.
      runtime.requirePublisher();
      if (update == null || update.entries() == null) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("loc", java.util.List.of("body", "entries"));
        error.put("msg", "field required");
        error.put("type", "value_error.missing");
        return Responses.detail(
            StatusCodes.UNPROCESSABLE_CONTENT, java.util.Map.of("detail", java.util.List.of(error)));
      }
      try {
        Map<String, Object> claims = Authn.requireLoggedIn(runtime.signer(), requestContext());
        Authz.requirePeerType(runtime.signer().enabled(), claims, PeerType.datasource);
        Authz.restrictOptionalTopicsToPublish(runtime.signer().enabled(), claims, update);
      } catch (Unauthorized e) {
        log.error("Unauthorized to publish update: {}", e.getMessage());
        return Responses.unauthorized(e);
      }
      runtime.publishDataUpdate(update, null);
      return Responses.statusOk();
    });
  }

  /** R132: a client's completion report, logged with the payload-bearing fields removed. */
  @Post("/data/callback_report")
  public HttpResponse logClientUpdateReport(Data.DataUpdateReport report) {
    return Responses.guarded(requestContext(), () -> {
      if (!Role.isServer()) {
        return Responses.notFound();
      }
      if (report == null || report.reports() == null) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("loc", java.util.List.of("body", "reports"));
        error.put("msg", "field required");
        error.put("type", "value_error.missing");
        return Responses.detail(
            StatusCodes.UNPROCESSABLE_CONTENT, java.util.Map.of("detail", java.util.List.of(error)));
      }
      log.info(
          "Received update report for update {} with {} entries",
          report.update_id(),
          report.reports().size());
      return Responses.ok(Map.of());
    });
  }

  static String withQueryParam(String url, String name, String value) {
    if (value == null) {
      return url;
    }
    return io.akka.opal.common.util.Urls.setUrlQueryParam(url, name, value);
  }
  /**
   * R193: the three data routes answer wherever their configuration entries put them.
   *
   * <p>The source mounts each of these routers at a path read from configuration, so a
   * deployment can move them; here the paths are on annotations, which are fixed when the code
   * is compiled. These catch what the fixed paths did not and dispatch on the configured value,
   * so a moved route answers as well as the default one.
   *
   * <p>Everything else is a 404, which is what an unrouted path already answered. One route per
   * depth rather than one wildcard: the runtime refuses a wildcard beside a fixed route at the
   * same level, and every path OPAL mounts is one, two or three segments deep.
   */
  @Get("/{first}")
  public HttpResponse configuredGetOne(String first) {
    return Responses.guarded(requestContext(), () -> configuredGet("/" + first));
  }

  @Get("/{first}/{second}")
  public HttpResponse configuredGetTwo(String first, String second) {
    return Responses.guarded(requestContext(), () -> configuredGet("/" + first + "/" + second));
  }

  @Get("/{first}/{second}/{third}")
  public HttpResponse configuredGetThree(String first, String second, String third) {
    return Responses.guarded(
        requestContext(), () -> configuredGet("/" + first + "/" + second + "/" + third));
  }

  private HttpResponse configuredGet(String path) {
    if (!Role.isServer()) {
      return Responses.notFound();
    }
    if (matchesConfigured(path, "ALL_DATA_ROUTE", "/policy-data")) {
      return defaultAllData();
    }
    if (matchesConfigured(path, "DATA_CONFIG_ROUTE", "/data/config")) {
      return getDataSourcesConfig();
    }
    return Responses.notFound();
  }

  /**
   * The same for the two routes a caller posts to.
   *
   * <p>Three arities rather than one wildcard, because a wildcard leaves no parameter for the
   * body and a posted route has one. Every path OPAL mounts is one, two or three segments deep.
   */
  @Post("/{first}")
  public HttpResponse configuredPostOne(String first, com.fasterxml.jackson.databind.JsonNode body) {
    return Responses.guarded(requestContext(), () -> configuredPost("/" + first, body));
  }

  @Post("/{first}/{second}")
  public HttpResponse configuredPostTwo(
      String first, String second, com.fasterxml.jackson.databind.JsonNode body) {
    return Responses.guarded(requestContext(), () -> configuredPost("/" + first + "/" + second, body));
  }

  @Post("/{first}/{second}/{third}")
  public HttpResponse configuredPostThree(
      String first, String second, String third, com.fasterxml.jackson.databind.JsonNode body) {
    return Responses.guarded(requestContext(), () -> configuredPost("/" + first + "/" + second + "/" + third, body));
  }

  /**
   * The body arrives untyped because one method cannot declare two body types, and is read as
   * whichever of the two the matched route expects.
   */
  private HttpResponse configuredPost(String path, com.fasterxml.jackson.databind.JsonNode body) {
    if (!Role.isServer()) {
      return Responses.notFound();
    }
    if (matchesConfigured(path, "DATA_CONFIG_ROUTE", "/data/config")) {
      return publishDataUpdateEvent(read(body, Data.DataUpdate.class));
    }
    if (matchesConfigured(path, "DATA_CALLBACK_DEFAULT_ROUTE", "/data/callback_report")) {
      return logClientUpdateReport(read(body, Data.DataUpdateReport.class));
    }
    return Responses.notFound();
  }

  private static <T> T read(com.fasterxml.jackson.databind.JsonNode body, Class<T> type) {
    return io.akka.opal.server.pubsub.Rpc.MAPPER.convertValue(body, type);
  }

  /**
   * Whether the request is on a route a deployment moved.
   *
   * <p>The default is excluded because the annotation above already answers it; matching it here
   * as well would mean two routes claiming one path.
   */
  private boolean matchesConfigured(String path, String entry, String compiledDefault) {
    String configured = runtime.config().getString(entry);
    return configured != null && !configured.equals(compiledDefault) && configured.equals(path);
  }
}
