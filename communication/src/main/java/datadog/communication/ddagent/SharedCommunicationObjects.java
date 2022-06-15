package datadog.communication.ddagent;

import datadog.common.socket.SocketUtils;
import datadog.communication.http.OkHttpUtils;
import datadog.communication.monitor.Monitoring;
import datadog.remote_config.ConfigurationPoller;
import datadog.trace.api.Config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.slf4j.LoggerFactory;

public class SharedCommunicationObjects {
  public OkHttpClient okHttpClient;
  public HttpUrl agentUrl;
  public Monitoring monitoring;
  public DDAgentFeaturesDiscovery featuresDiscovery;
  public ConfigurationPoller configurationPoller;

  public void createRemaining(Config config) {
    if (monitoring == null) {
      monitoring = Monitoring.DISABLED;
    }
    if (agentUrl == null) {
      agentUrl = HttpUrl.parse(config.getAgentUrl());
    }
    if (okHttpClient == null) {
      String unixDomainSocket = SocketUtils.discoverApmSocket(config);
      String namedPipe = config.getAgentNamedPipe();
      okHttpClient =
          OkHttpUtils.buildHttpClient(
              agentUrl,
              unixDomainSocket,
              namedPipe,
              TimeUnit.SECONDS.toMillis(config.getAgentTimeout()));
    }
    featuresDiscovery(config);
    if (configurationPoller == null && config.isRemoteConfigEnabled()) {
      String remoteConfigUrl = config.getFinalRemoteConfigUrl();
      if (remoteConfigUrl != null) {
        configurationPoller = new ConfigurationPoller(config, getTracerVersion(), remoteConfigUrl, okHttpClient);
      } else {
        String configEndpoint = featuresDiscovery.getConfigEndpoint();
        if (configEndpoint == null) {
          featuresDiscovery.discover();
          configEndpoint = featuresDiscovery.getConfigEndpoint();
        }
        if (configEndpoint != null) {
          remoteConfigUrl =  featuresDiscovery.buildUrl(configEndpoint).toString();
          configurationPoller = new ConfigurationPoller(config, getTracerVersion(), remoteConfigUrl, okHttpClient);
        }
      }
    }
  }

  public DDAgentFeaturesDiscovery featuresDiscovery(Config config) {
    if (featuresDiscovery == null) {
      featuresDiscovery =
          new DDAgentFeaturesDiscovery(
              okHttpClient,
              monitoring,
              agentUrl,
              config.isTraceAgentV05Enabled(),
              config.isTracerMetricsEnabled());
    }
    return featuresDiscovery;
  }

  private static String getTracerVersion() {
    final StringBuilder sb = new StringBuilder(32);
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try (final BufferedReader reader =
             new BufferedReader(
                 new InputStreamReader(
                     cl.getResourceAsStream("dd-java-agent.version"), StandardCharsets.ISO_8859_1))) {
      for (int c = reader.read(); c != -1; c = reader.read()) {
        sb.append((char) c);
      }
    } catch (IOException e) {
      return "0.0.0";
    }

    return sb.toString().trim();
  }
}
