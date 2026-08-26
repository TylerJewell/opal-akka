package io.akka.opal.common.util;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The AWS Signature Version 4 headers a bundle fetched from S3 carries — SPEC-002 R42.
 *
 * <p>The payload hash is the digest of the empty string because the request is a GET with no
 * body, which is the only shape OPAL signs.
 */
public final class Aws {

  private static final String SHA256_EMPTY =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  private static final DateTimeFormatter AMZ_DATE =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter DATE_STAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

  private Aws() {}

  public static Map<String, String> restAuthHeaders(
      String keyId, String secretKey, String host, String path, String region) {
    return restAuthHeaders(keyId, secretKey, host, path, region, Instant.now());
  }

  static Map<String, String> restAuthHeaders(
      String keyId, String secretKey, String host, String path, String region, Instant now) {
    String amzDate = AMZ_DATE.format(now);
    String dateStamp = DATE_STAMP.format(now);

    String canonicalHeaders = "host:" + host + "\n" + "x-amz-date:" + amzDate + "\n";
    String signedHeaders = "host;x-amz-date";
    String payloadHash = Hashing.sha256("");

    String canonicalRequest =
        "GET" + "\n" + path + "\n" + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n"
            + payloadHash;

    String algorithm = "AWS4-HMAC-SHA256";
    String credentialScope = dateStamp + "/" + region + "/" + "s3" + "/" + "aws4_request";
    String stringToSign =
        algorithm + "\n" + amzDate + "\n" + credentialScope + "\n" + Hashing.sha256(canonicalRequest);

    byte[] signingKey = signatureKey(secretKey, dateStamp, region, "s3");
    String signature = Hashing.hex(hmac(signingKey, stringToSign));

    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("x-amz-date", amzDate);
    headers.put("x-amz-content-sha256", SHA256_EMPTY);
    headers.put(
        "Authorization",
        algorithm
            + " "
            + "Credential="
            + keyId
            + "/"
            + credentialScope
            + ", "
            + "SignedHeaders="
            + signedHeaders
            + ", "
            + "Signature="
            + signature);
    return headers;
  }

  static byte[] signatureKey(String key, String dateStamp, String regionName, String serviceName) {
    byte[] kDate = hmac(("AWS4" + key).getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = hmac(kDate, regionName);
    byte[] kService = hmac(kRegion, serviceName);
    return hmac(kService, "aws4_request");
  }

  public static byte[] hmac(byte[] key, String message) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static byte[] hmac(byte[] key, byte[] message) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(message);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
