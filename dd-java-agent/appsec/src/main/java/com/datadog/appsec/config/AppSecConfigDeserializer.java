package com.datadog.appsec.config;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.Types;
import datadog.remote_config.ConfigurationDeserializer;
import okio.Okio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class AppSecConfigDeserializer implements ConfigurationDeserializer<AppSecConfig> {
  public static final AppSecConfigDeserializer INSTANCE = new AppSecConfigDeserializer();

  private static final JsonAdapter<Map<String, Object>> ADAPTER =
      new Moshi.Builder()
          .build()
          .adapter(Types.newParameterizedType(Map.class, String.class, Object.class));

  private AppSecConfigDeserializer() {}

  @Override
  public AppSecConfig deserialize(byte[] content) {
    return deserialize(new ByteArrayInputStream(content));
  }

  public AppSecConfig deserialize(InputStream is) {
    try {
      Map<String, Object> configMap = ADAPTER.fromJson(Okio.buffer(Okio.source(is)));

      return AppSecConfig.valueOf(configMap);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
