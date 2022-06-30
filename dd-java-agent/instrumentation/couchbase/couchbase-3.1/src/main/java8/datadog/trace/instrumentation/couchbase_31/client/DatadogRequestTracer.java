package datadog.trace.instrumentation.couchbase_31.client;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import datadog.trace.api.DDSpanTypes;
import datadog.trace.bootstrap.instrumentation.api.AgentSpan;
import datadog.trace.bootstrap.instrumentation.api.AgentTracer;
import datadog.trace.bootstrap.instrumentation.api.Tags;
import java.time.Duration;
import reactor.core.publisher.Mono;

public class DatadogRequestTracer implements RequestTracer {
  private final AgentTracer.TracerAPI tracer;

  public DatadogRequestTracer(AgentTracer.TracerAPI tracer) {
    this.tracer = tracer;
  }

  @Override
  public RequestSpan requestSpan(String requestName, RequestSpan requestParent) {
    AgentTracer.SpanBuilder builder = tracer.buildSpan(requestName);
    AgentSpan parent = DatadogRequestSpan.unwrap(requestParent);
    if (parent == null) {
      parent = tracer.activeSpan();
    }
    if (parent != null) {
      builder.asChildOf(parent.context());
    }
    builder.withServiceName("couchbase");
    builder.withSpanType(DDSpanTypes.COUCHBASE);
    builder.withTag(Tags.COMPONENT, "couchbase-client");
    builder.withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CLIENT);
    builder.withTag(Tags.DB_TYPE, "couchbase");
    return DatadogRequestSpan.wrap(builder.start());
  }

  @Override
  public Mono<Void> start() {
    return Mono.empty(); // Tracer already exists
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return Mono.empty(); // Tracer should continue to exist
  }
}
