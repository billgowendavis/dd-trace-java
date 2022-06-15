package datadog.remote_config;

import javax.annotation.Nullable;
import java.time.Duration;

public interface ConfigurationChangesListener<T> {
  boolean accept(@Nullable T configuration, // null to "unapply" the configuration
                 PollingRateHinter pollingRateHinter);

  interface PollingRateHinter {
    PollingRateHinter NOOP = new PollingHinterNoop();

    void suggestPollingRate(Duration duration);
  }

  class PollingHinterNoop implements PollingRateHinter {
    @Override
    public void suggestPollingRate(Duration duration) {
    }
  }
}
