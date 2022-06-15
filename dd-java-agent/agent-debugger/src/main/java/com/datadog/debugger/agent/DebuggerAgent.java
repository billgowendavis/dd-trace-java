package com.datadog.debugger.agent;

import static datadog.communication.http.OkHttpUtils.buildHttpClient;
import static datadog.trace.util.AgentThreadFactory.AGENT_THREAD_GROUP;

import com.datadog.debugger.sink.DebuggerSink;
import com.datadog.debugger.uploader.BatchUploader;
import datadog.communication.ddagent.DDAgentFeaturesDiscovery;
import datadog.communication.ddagent.SharedCommunicationObjects;
import datadog.communication.monitor.Monitoring;
import datadog.remote_config.ConfigurationPoller;
import datadog.remote_config.Product;
import datadog.trace.api.Config;
import datadog.trace.bootstrap.debugger.DebuggerContext;
import datadog.trace.bootstrap.debugger.Snapshot;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Debugger agent implementation */
public class DebuggerAgent {
  private static final Logger log = LoggerFactory.getLogger(DebuggerAgent.class);
  private static ConfigurationPoller configurationPoller;
  private static DebuggerSink sink;
  private static String agentVersion;

  public static synchronized void run(Instrumentation instrumentation, SharedCommunicationObjects sco) {
    Config config = Config.get();
    if (!config.isDebuggerEnabled()) {
      log.info("Debugger agent disabled");
      return;
    }

    String finalDebuggerSnapshotUrl = config.getFinalDebuggerSnapshotUrl();
    String agentUrl = config.getAgentUrl();
    boolean isSnapshotUploadThroughAgent = Objects.equals(finalDebuggerSnapshotUrl, agentUrl);
    agentVersion = sco.featuresDiscovery.getVersion();
    if (isSnapshotUploadThroughAgent && !sco.featuresDiscovery.supportsDebugger()) {
      log.error(
          "No endpoint detected to upload snapshots to from datadog agent at "
              + agentUrl
              + ". Consider upgrading the datadog agent.");
      return;
    }
    if (!config.isRemoteConfigEnabled()) {
      log.error("Remote config is disabled; debugger agent cannot work");
      return;
    }
    configurationPoller = sco.configurationPoller;
    if (configurationPoller == null) {
      log.error(
          "No endpoint detected to read probe config from datadog agent at "
              + agentUrl
              + ". Consider upgrading the datadog agent.");
      return;
    }

    sink = new DebuggerSink(config);
    sink.start();
    ConfigurationUpdater configurationUpdater =
        new ConfigurationUpdater(
            instrumentation, DebuggerAgent::createTransformer, config, sink);
    StatsdMetricForwarder statsdMetricForwarder = new StatsdMetricForwarder(config);
    DebuggerContext.init(sink, configurationUpdater, statsdMetricForwarder);
    DebuggerContext.initClassFilter(new DenyListHelper(null)); // default hard coded deny list
    if (config.isDebuggerInstrumentTheWorld()) {
      setupInstrumentTheWorldTransformer(
          config, instrumentation, sink, statsdMetricForwarder);
    }


    LiveDebuggingConfigDeserializer deserializer = new LiveDebuggingConfigDeserializer(config.getServiceName());
    String probeFileLocation = config.getDebuggerProbeFileLocation();
    if (probeFileLocation != null) {
      Path probeFilePath = Paths.get(probeFileLocation);
      configurationPoller.addFileListener(probeFilePath.toFile(), deserializer, configurationUpdater);
    } else {
      configurationPoller.addListener(Product.LIVE_DEBUGGING,
          deserializer,
          configurationUpdater);
    }

    try {
      /*
      Note: shutdown hooks are tricky because JVM holds reference for them forever preventing
      GC for anything that is reachable from it.
       */
      Runtime.getRuntime()
          .addShutdownHook(new ShutdownHook(sink.getSnapshotUploader()));
    } catch (final IllegalStateException ex) {
      // The JVM is already shutting down.
    }
  }

  static ClassFileTransformer setupInstrumentTheWorldTransformer(
      Config config,
      Instrumentation instrumentation,
      DebuggerContext.Sink sink,
      StatsdMetricForwarder statsdMetricForwarder) {
    log.info("install Instrument-The-World transformer");
    DebuggerContext.init(sink, DebuggerAgent::instrumentTheWorldResolver, statsdMetricForwarder);
    DebuggerTransformer transformer =
        createTransformer(config, new Configuration("", -1, Collections.emptyList()), null);
    instrumentation.addTransformer(transformer);
    return transformer;
  }

  public static String getAgentVersion() {
    return agentVersion;
  }

  private static DebuggerTransformer createTransformer(
      Config config,
      Configuration configuration,
      DebuggerTransformer.InstrumentationListener listener) {
    return new DebuggerTransformer(config, configuration, listener);
  }

  private static Snapshot.ProbeDetails instrumentTheWorldResolver(
      String id, Class<?> callingClass) {
    return Snapshot.ProbeDetails.ITW_PROBE;
  }

  static void stop() {
    configurationPoller.removeListener(Product.LIVE_DEBUGGING);
    if (sink != null) {
      sink.stop();
    }
  }

  private static class ShutdownHook extends Thread {

    private final WeakReference<BatchUploader> uploaderRef;

    private ShutdownHook(BatchUploader uploader) {
      super(AGENT_THREAD_GROUP, "dd-debugger-shutdown-hook");
      uploaderRef = new WeakReference<>(uploader);
    }

    @Override
    public void run() {
      final BatchUploader uploader = uploaderRef.get();
      if (uploader != null) {
        try {
          uploader.shutdown();
        } catch (Exception ex) {
          log.warn("Failed to shutdown SnapshotUploader", ex);
        }
      }
    }
  }
}
