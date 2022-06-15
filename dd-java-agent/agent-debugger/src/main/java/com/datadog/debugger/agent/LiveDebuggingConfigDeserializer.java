package com.datadog.debugger.agent;

import com.datadog.debugger.util.MoshiHelper;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import datadog.remote_config.ConfigurationDeserializer;
import datadog.trace.relocate.api.RatelimitedLogger;
import datadog.trace.util.ExceptionHelper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveDebuggingConfigDeserializer implements ConfigurationDeserializer<Configuration> {
  private static final int MINUTES_BETWEEN_ERROR_LOG = 5;

  private final static Logger log = LoggerFactory.getLogger(LiveDebuggingConfigDeserializer.class);
  private final static RatelimitedLogger RATE_LIMITED_LOGGER =
        new RatelimitedLogger(log, MINUTES_BETWEEN_ERROR_LOG, TimeUnit.MINUTES);

  private final static Moshi MOSHI = MoshiHelper.createMoshiConfig();
  private final String expectedServiceName;

  public LiveDebuggingConfigDeserializer(String expectedServiceName) {
    this.expectedServiceName = expectedServiceName;
  }

  @Override
  public Configuration deserialize(byte[] content) {
    Configuration config;
    try {
      String configStr = new String(content);
      JsonAdapter<Configuration> adapter = MOSHI.adapter(Configuration.class);
      config = adapter.fromJson(configStr);
    } catch (IOException ex) {
      ExceptionHelper.rateLimitedLogException(
          RATE_LIMITED_LOGGER,
          log,
          ex,
          "Failed to deserialize configuration {}",
          new String(content, StandardCharsets.UTF_8));
      return null;
    }
    if (config.getId().equals(expectedServiceName)) {
      return config;
    } else {
      log.warn("configuration id mismatch, expected {} but got {}", expectedServiceName, config.getId());
    }
    return null;
  }
}
