import com.couchbase.client.core.env.TimeoutConfig
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.java.Cluster
import com.couchbase.client.java.ClusterOptions
import com.couchbase.client.java.Collection
import com.couchbase.client.java.env.ClusterEnvironment
import datadog.trace.agent.test.AgentTestRunner
import datadog.trace.agent.test.asserts.TraceAssert
import datadog.trace.api.DDSpanTypes
import datadog.trace.bootstrap.instrumentation.api.Tags
import datadog.trace.core.DDSpan
import org.testcontainers.couchbase.BucketDefinition
import org.testcontainers.couchbase.CouchbaseContainer
import spock.lang.Shared

import java.time.Duration

class CouchbaseClient32Test extends AgentTestRunner {
  @Shared
  CouchbaseContainer couchbase
  @Shared
  Cluster cluster
  @Shared
  Collection collection

  def setupSpec() {
    def arch = System.getProperty("os.arch") == "aarch64" ? "-aarch64" : ""
    couchbase = new CouchbaseContainer("couchbase/server:7.1.0${arch}")
      .withBucket(new BucketDefinition("test"))
      .withStartupTimeout(Duration.ofSeconds(120))
    couchbase.start()

    ClusterEnvironment environment = ClusterEnvironment.builder()
      .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(10)))
      .build()

    def connectionString = "couchbase://${couchbase.host}:${couchbase.bootstrapCarrierDirectPort},${couchbase.host}:${couchbase.bootstrapHttpDirectPort}=manager"
    cluster = Cluster.connect(connectionString, ClusterOptions
      .clusterOptions(couchbase.username, couchbase.password)
      .environment(environment))
    def bucket = cluster.bucket("test")
    bucket.waitUntilReady(Duration.ofSeconds(30))
    collection = bucket.defaultCollection()
  }

  def cleanupSpec() {
    couchbase.stop()
  }

  def "check spans"() {
    when:
    try {
      collection.get("not_found")
    } catch (DocumentNotFoundException ignored) {
      // expected exception
    }

    then:
    assertTraces(1) {
      trace(2) {
        assertCouchbaseCall(it, "get", [
          'db.couchbase.collection' : '_default',
          'db.couchbase.document_id': { String },
          'db.couchbase.retries'    : { Long },
          'db.couchbase.scope'      : '_default',
          'db.couchbase.service'    : 'kv',
          'db.name'                 : 'test',
          'db.operation'            : 'get',
          'db.system'               : 'couchbase'
        ])
        assertCouchbaseCall(it, "dispatch_to_server", [
          'db.couchbase.collection'     : '_default',
          'db.couchbase.document_id'    : { String },
          'db.couchbase.local_id'       : { String },
          'db.couchbase.operation_id'   : { String },
          'db.couchbase.scope'          : '_default',
          'db.couchbase.server_duration': { Long },
          'db.name'                     : 'test',
          'net.host.name'               : { String },
          'net.host.port'               : { Long },
          'net.peer.name'               : { String },
          'net.peer.port'               : { Long },
          'net.transport'               : { String },
          'db.system'                   : 'couchbase'
        ], span(0))
      }
    }

    cleanup:
    cluster.disconnect()
  }

  void assertCouchbaseCall(TraceAssert trace, String name, Map<String, Serializable> extraTags, Object parentSpan = null) {
    trace.span {
      serviceName "couchbase"
      resourceName name
      operationName name
      spanType DDSpanTypes.COUCHBASE
      errored false
      if (parentSpan == null) {
        parent()
      } else {
        childOf((DDSpan) parentSpan)
      }
      tags {
        "$Tags.COMPONENT" "couchbase-client"
        "$Tags.SPAN_KIND" Tags.SPAN_KIND_CLIENT
        "$Tags.DB_TYPE" "couchbase"
        if (extraTags != null) {
          addTags(extraTags)
        }
        defaultTags()
      }
    }
  }
}
