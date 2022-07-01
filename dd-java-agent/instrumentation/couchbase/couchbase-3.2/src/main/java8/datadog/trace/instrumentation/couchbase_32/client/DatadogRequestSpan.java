package datadog.trace.instrumentation.couchbase_32.client;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.RequestContext;
import datadog.trace.bootstrap.instrumentation.api.AgentSpan;
import java.time.Instant;

public class DatadogRequestSpan implements RequestSpan {
  private final AgentSpan span;

  private DatadogRequestSpan(AgentSpan span) {
    this.span = span;
  }

  public static RequestSpan wrap(AgentSpan span) {
    return new DatadogRequestSpan(span);
  }

  public static AgentSpan unwrap(RequestSpan span) {
    if (span == null) {
      return null;
    }
    if (span instanceof DatadogRequestSpan) {
      return ((DatadogRequestSpan) span).span;
    } else {
      throw new IllegalArgumentException("RequestSpan must be of type DatadogRequestSpan");
    }
  }

  @Override
  public void attribute(String key, String value) {
    span.setTag(key, value);
  }

  @Override
  public void attribute(String key, boolean value) {
    span.setTag(key, value);
  }

  @Override
  public void attribute(String key, long value) {
    span.setTag(key, value);
  }

  @Override
  public void event(String name, Instant timestamp) {
    // TODO event support would be nice
  }

  @Override
  public void status(StatusCode status) {
    if (status == null) {
      return;
    }
    switch (status) {
      case OK:
        span.setError(false);
        break;
      case ERROR:
        span.setError(true);
        break;
      default:
    }
  }

  @Override
  public void end() {
    span.finish();
  }

  @Override
  public void requestContext(RequestContext requestContext) {
    // TODO should we add tags/metrics based on the request context when the span ends?
  }
}
