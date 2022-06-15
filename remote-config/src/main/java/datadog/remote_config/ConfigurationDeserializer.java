package datadog.remote_config;

public interface ConfigurationDeserializer<T> {
  T deserialize(byte[] content);
}
