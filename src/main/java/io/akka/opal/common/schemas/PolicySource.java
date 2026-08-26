package io.akka.opal.common.schemas;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

/** A scope's policy source and its authentication — SPEC-002 section 2.3. */
public final class PolicySource {

  private PolicySource() {}

  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.EXISTING_PROPERTY,
      property = "auth_type",
      visible = true,
      defaultImpl = NoAuthData.class)
  @JsonSubTypes({
    @JsonSubTypes.Type(value = NoAuthData.class, name = "none"),
    @JsonSubTypes.Type(value = SSHAuthData.class, name = "ssh"),
    @JsonSubTypes.Type(value = GitHubTokenAuthData.class, name = "github_token"),
    @JsonSubTypes.Type(value = UserPassAuthData.class, name = "userpass")
  })
  public sealed interface AuthData
      permits NoAuthData, SSHAuthData, GitHubTokenAuthData, UserPassAuthData {
    String auth_type();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record NoAuthData(String auth_type) implements AuthData {
    public NoAuthData {
      auth_type = "none";
    }

    public static NoAuthData get() {
      return new NoAuthData("none");
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SSHAuthData(
      String auth_type, String username, String public_key, String private_key)
      implements AuthData {
    public SSHAuthData {
      auth_type = "ssh";
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record GitHubTokenAuthData(String auth_type, String token) implements AuthData {
    public GitHubTokenAuthData {
      auth_type = "github_token";
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record UserPassAuthData(String auth_type, String username, String password)
      implements AuthData {
    public UserPassAuthData {
      auth_type = "userpass";
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record GitPolicyScopeSource(
      String source_type,
      String url,
      AuthData auth,
      List<String> directories,
      List<String> extensions,
      List<String> bundle_ignore,
      String manifest,
      Boolean poll_updates,
      String branch) {

    public GitPolicyScopeSource {
      if (source_type == null) {
        source_type = "git";
      }
      if (auth == null) {
        auth = NoAuthData.get();
      }
      if (directories == null) {
        directories = List.of(".");
      }
      if (extensions == null) {
        extensions = List.of(".rego", ".json");
      }
      if (manifest == null) {
        manifest = ".manifest";
      }
      if (poll_updates == null) {
        poll_updates = false;
      }
      if (branch == null) {
        branch = "main";
      }
    }

    /** The source's identity for clone sharing — R111. Two scopes matching here share a clone. */
    public String fingerprint() {
      return url + "|" + branch + "|" + authFingerprint();
    }

    private String authFingerprint() {
      return switch (auth) {
        case NoAuthData ignored -> "none";
        case SSHAuthData a -> "ssh:" + a.username() + ":" + a.private_key();
        case GitHubTokenAuthData a -> "github_token:" + a.token();
        case UserPassAuthData a -> "userpass:" + a.username() + ":" + a.password();
      };
    }
  }
}
