package io.akka.opal.common.sources;

import io.akka.opal.common.config.Enums.PolicyBundleServerType;
import io.akka.opal.common.git.TarToLocalGit;
import io.akka.opal.common.util.Aws;
import io.akka.opal.common.util.Hashing;
import io.akka.opal.common.util.Http;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OPA bundle server watched for new bundles — SPEC-002 R40, R41 and R42.
 *
 * <p>Change is detected by ETag where the server offers one and by a SHA-256 of the downloaded
 * file where it does not. The second is a fallback rather than a preference: it downloads the
 * whole bundle to find out nothing changed, which is exactly what the ETag avoids.
 */
public final class ApiPolicySource extends PolicySource {

  private static final Logger log = LoggerFactory.getLogger(ApiPolicySource.class);

  private final String token;
  private final String tokenId;
  private final String region;
  private final PolicyBundleServerType serverType;
  private final Path tmpBundlePath;
  private final TarToLocalGit tarToGit;

  private String bundleHash;
  private String etag;
  private Git localGit;

  public ApiPolicySource(
      String remoteSourceUrl,
      String localClonePath,
      int pollingInterval,
      String token,
      String tokenId,
      String region,
      PolicyBundleServerType serverType,
      String policyBundlePath,
      String policyBundleGitAddPattern) {
    super(remoteSourceUrl, localClonePath, pollingInterval);
    this.token = token;
    this.tokenId = tokenId;
    this.region = region;
    this.serverType = serverType;
    this.tmpBundlePath = Path.of(policyBundlePath);
    this.tarToGit =
        new TarToLocalGit(this.localClonePath, policyBundlePath, policyBundleGitAddPattern);
  }

  public Repository repository() {
    return localGit == null ? null : localGit.getRepository();
  }

  public boolean ready() {
    return localGit != null;
  }

  @Override
  public void getInitialPolicyStateFromRemote() {
    try {
      fetchPolicyBundle();
      localGit = tarToGit.createLocalGit();
    } catch (Exception e) {
      log.error("Failed to load initial policy from remote API bundle server", e);
      fireFailure(e instanceof Exception ? e : new IllegalStateException(e));
    }
  }

  @Override
  protected void releaseRepository() {
    if (localGit != null) {
      localGit.close();
      localGit = null;
    }
  }

  @Override
  public void checkForChanges() {
    try {
      apiUpdatePolicy();
    } catch (Exception e) {
      log.error("Failed to update policy from remote API bundle server", e);
      fireFailure(e);
    }
  }

  /** Whether the bundle moved on to a new version, and the version it is on now. */
  record Updated(boolean changed, String version) {}

  /**
   * R40, R41: a fetch that found different bytes is an update only when there was a previous
   * version to move on from.
   *
   * <p>A bundle server that starts sending ETags mid-life, and the very first fetch of any
   * server, both produce "these bytes differ from the nothing I had before" — and committing
   * that would put a second commit carrying the first bundle's own content into the repository
   * the bundle route serves, which every connected client would then be told to fetch.
   */
  Updated apiUpdatePolicy() throws Exception {
    Fetched fetched = fetchPolicyBundle();
    if (!fetched.changed() || fetched.previousVersion() == null) {
      return new Updated(false, fetched.currentHash());
    }
    TarToLocalGit.Committed committed =
        tarToGit.extractBundleToLocalGit("new version " + fetched.currentHash());
    localGit = committed.git();
    ObjectId previous = committed.previous();
    if (previous != null) {
      fireNewPolicy(previous, committed.latest());
    }
    return new Updated(true, fetched.currentHash());
  }

  /** Whether the bundle moved, and the hash the server or the file itself reports. */
  record Fetched(boolean changed, String previousVersion, String currentHash) {}

  Fetched fetchPolicyBundle() throws Exception {
    String path = "bundle.tar.gz";
    String fullUrl = remoteSourceUrl + "/" + path;
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(fullUrl))
            .header("content-type", "application/gzip")
            .GET();
    buildAuthHeaders(path).forEach(request::header);
    if (etag != null) {
      request.header("ETag", etag).header("If-None-Match", etag);
    }
    HttpResponse<byte[]> response =
        Http.plain().send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    if (response.statusCode() == 404) {
      log.warn("requested url not found: {}", fullUrl);
      throw new IllegalStateException("requested url not found: " + fullUrl);
    }
    if (response.statusCode() == 304) {
      log.info("Not modified");
      return new Fetched(false, null, etag);
    }
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "unexpected response code while fetching bundle: " + response.statusCode());
    }
    String currentEtag = response.headers().firstValue("ETag").orElse(null);
    Files.createDirectories(tmpBundlePath.toAbsolutePath().getParent());
    Files.write(tmpBundlePath, response.body());

    if (currentEtag == null) {
      log.info("Etag is turned off, you may want to turn it on at your bundle server");
      String currentBundleHash = Hashing.sha256(response.body());
      log.info("Bundle hash is {}", currentBundleHash);
      if (currentBundleHash.equals(bundleHash)) {
        log.info("No new bundle, hash is: {}", currentBundleHash);
        return new Fetched(false, null, currentBundleHash);
      }
      String previous = bundleHash;
      bundleHash = currentBundleHash;
      return new Fetched(true, previous, currentBundleHash);
    }
    String previous = etag;
    if (currentEtag.equals(etag)) {
      return new Fetched(false, null, currentEtag);
    }
    etag = currentEtag;
    return new Fetched(true, previous, currentEtag);
  }

  /** R42: a bearer token for HTTP, and a SigV4 triple for S3. */
  Map<String, String> buildAuthHeaders(String path) {
    Map<String, String> headers = new LinkedHashMap<>();
    if (serverType == PolicyBundleServerType.HTTP && token != null) {
      headers.put("Authorization", "Bearer " + token);
      return headers;
    }
    if (serverType == PolicyBundleServerType.AWS_S3 && token != null && tokenId != null) {
      URI parsed = URI.create(remoteSourceUrl);
      String host = parsed.getAuthority();
      String fullPath = parsed.getPath() + "/" + path;
      return Aws.restAuthHeaders(tokenId, token, host, fullPath, region);
    }
    return headers;
  }
}
