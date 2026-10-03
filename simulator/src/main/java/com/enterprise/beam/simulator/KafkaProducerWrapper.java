package com.enterprise.beam.simulator;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.util.JsonUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Wrapper for Kafka Producer with custom serialization and error callbacks.
 */
public class KafkaProducerWrapper implements AutoCloseable {

  private final Producer<String, String> producer;

  public KafkaProducerWrapper(String bootstrapServers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // LAPTOP_CONSTRAINT: Linger & batch settings optimized for local low latency
    // ENTERPRISE_UPGRADE: High throughput configuration (linger.ms=5, batch.size=65536, compression.type=lz4)
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.ACKS_CONFIG, "1");

    this.producer = new KafkaProducer<>(props);
  }

  public Future<RecordMetadata> send(String topic, BaseEvent event) throws Exception {
    String json = JsonUtils.getMapper().writeValueAsString(event);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getCustomerId(), json);
    return producer.send(record);
  }

  public Future<RecordMetadata> sendRaw(String topic, String key, String rawJson) {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, rawJson);
    return producer.send(record);
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.flush();
      producer.close();
    }
  }
}
