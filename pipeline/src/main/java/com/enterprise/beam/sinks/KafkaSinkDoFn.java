package com.enterprise.beam.sinks;

import com.enterprise.beam.config.PipelineConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe Beam DoFn for streaming key/value records directly to Apache Kafka topics.
 * Guarantees at-least-once delivery by tracking async delivery callbacks and flushing in {@code @FinishBundle}.
 */
public class KafkaSinkDoFn extends DoFn<KV<String, String>, Void> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkDoFn.class);

  private final PipelineConfig config;
  private final String bootstrapServers;
  private final String topic;
  private transient KafkaProducer<String, String> producer;
  private transient AtomicReference<Throwable> firstAsyncError;

  public KafkaSinkDoFn(PipelineConfig config, String topic) {
    this.config = config;
    this.bootstrapServers = config.getKafkaBootstrapServers();
    this.topic = topic;
  }

  /** Millis of last init attempt; 0 = never attempted. Used to throttle reconnects in fail-open mode. */
  private transient volatile long lastInitAttemptMs = 0L;
  /** Reconnect interval for fail-open mode: at most once per 30 seconds. */
  static final long RECONNECT_THROTTLE_MS = 30_000L;
  /** Injectable clock supplier for testing. */
  private transient java.util.function.LongSupplier clockMs = System::currentTimeMillis;

  @Setup
  public void setup() {
    initProducer();
  }

  @StartBundle
  public void startBundle() {
    firstAsyncError = new AtomicReference<>(null);
  }

  private synchronized void initProducer() {
    if (producer != null) {
      return;
    }
    // A4: In fail-open mode, throttle reconnect attempts to once per RECONNECT_THROTTLE_MS.
    long nowMs = clockMs.getAsLong();
    if (config.isSinkFailOpen() && "LOCAL".equalsIgnoreCase(config.getEnvironment())) {
      if (lastInitAttemptMs > 0 && (nowMs - lastInitAttemptMs) < RECONNECT_THROTTLE_MS) {
        return; // throttled: too soon since last attempt
      }
      lastInitAttemptMs = nowMs;
    }
    try {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      // acks=all: waits for all in-sync replicas to acknowledge before considering the write
      // successful. Required for at-least-once delivery guarantee on risk alerts and DLQ.
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.RETRIES_CONFIG, 3);
      // enable.idempotence=true: prevents duplicate messages caused by producer retries.
      // Defaults to max.in.flight.requests.per.connection=5 which is safe with idempotence.
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
      // Timeout settings from PipelineConfig. LOCAL defaults are short to fail fast in tests.
      // Production defaults: delivery 120s, request 30s, block 60s.
      props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) config.getKafkaProducerDeliveryTimeoutMs());
      props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) config.getKafkaProducerRequestTimeoutMs());
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.getKafkaProducerMaxBlockMs());

      // D2: Kafka security passthrough. Only applied when KAFKA_SECURITY_PROTOCOL is non-empty.
      // To enable SASL_SSL set KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, and
      // KAFKA_SASL_JAAS_CONFIG in the environment. SSL truststore fields are optional.
      String securityProtocol = config.getKafkaSecurityProtocol();
      if (securityProtocol != null && !securityProtocol.isEmpty()) {
        props.put("security.protocol", securityProtocol);
        String saslMechanism = config.getKafkaSaslMechanism();
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
          props.put("sasl.mechanism", saslMechanism);
        }
        String jaasConfig = config.getKafkaSaslJaasConfig();
        if (jaasConfig != null && !jaasConfig.isEmpty()) {
          props.put("sasl.jaas.config", jaasConfig);
        }
        String truststoreLocation = config.getKafkaSslTruststoreLocation();
        if (truststoreLocation != null && !truststoreLocation.isEmpty()) {
          props.put("ssl.truststore.location", truststoreLocation);
          props.put("ssl.truststore.password", config.getKafkaSslTruststorePassword());
        }
        LOG.info("KafkaProducer security enabled: protocol={} mechanism={}", securityProtocol, saslMechanism);
      }

      producer = new KafkaProducer<>(props);
      lastInitAttemptMs = 0L; // reset throttle on success
      LOG.info("KafkaProducer initialized for topic: {} at {}", topic, bootstrapServers);
    } catch (Exception e) {
      if (config.isSinkFailOpen() && "LOCAL".equalsIgnoreCase(config.getEnvironment())) {
        LOG.warn("[fail-open] Kafka init failed for topic {}; will retry after {}s: {}",
            topic, RECONNECT_THROTTLE_MS / 1000, e.getMessage());
      } else {
        LOG.error("Could not initialize KafkaProducer for topic {}: {}", topic, e.getMessage(), e);
        throw new RuntimeException("Failed to initialize KafkaProducer for topic " + topic + ": " + e.getMessage(), e);
      }
    }
  }

  @ProcessElement
  public void processElement(@Element KV<String, String> kv) throws Exception {
    if (producer == null) {
      initProducer();
    }
    if (producer == null) {
      if (config.isSinkFailOpen() && "LOCAL".equalsIgnoreCase(config.getEnvironment())) {
        LOG.warn("Skipping Kafka write because KafkaProducer is not initialized for topic {}", topic);
        return;
      }
      throw new IllegalStateException("KafkaProducer is not initialized for topic " + topic);
    }

    try {
      // Async send with a callback capturing any delivery error into firstAsyncError.
      producer.send(new ProducerRecord<>(topic, kv.getKey(), kv.getValue()), (metadata, exception) -> {
        if (exception != null) {
          LOG.error("Kafka delivery failure on topic {} partition {}: {}",
              topic, metadata != null ? metadata.partition() : -1, exception.getMessage(), exception);
          if (firstAsyncError != null) {
            firstAsyncError.compareAndSet(null, exception);
          }
        }
      });
    } catch (Exception e) {
      LOG.error("Failed to send record to Kafka topic {}: {}", topic, e.getMessage(), e);
      if (firstAsyncError != null) {
        firstAsyncError.compareAndSet(null, e);
      }
      throw e;
    }
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    if (producer != null) {
      producer.flush();
    }
    if (firstAsyncError != null && firstAsyncError.get() != null) {
      Throwable t = firstAsyncError.getAndSet(null);
      if (t instanceof Exception) {
        throw (Exception) t;
      }
      throw new RuntimeException("Kafka delivery failure: " + t.getMessage(), t);
    }
  }

  @Teardown
  public void teardown() {
    if (producer != null) {
      producer.close();
      LOG.info("KafkaProducer closed for topic: {}", topic);
    }
  }
}
