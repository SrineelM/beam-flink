package com.enterprise.beam.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Encapsulates a raw Kafka record payload alongside partition and offset metadata.
 */
public class RawRecord implements Serializable {

  private static final long serialVersionUID = 1L;

  private String payload;
  private String topic;
  private int partition;
  private long offset;
  private long kafkaTimestamp;

  public RawRecord() {
  }

  public RawRecord(String payload, String topic, int partition, long offset, long kafkaTimestamp) {
    this.payload = payload;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.kafkaTimestamp = kafkaTimestamp;
  }

  public String getPayload() { return payload; }
  public void setPayload(String payload) { this.payload = payload; }

  public String getTopic() { return topic; }
  public void setTopic(String topic) { this.topic = topic; }

  public int getPartition() { return partition; }
  public void setPartition(int partition) { this.partition = partition; }

  public long getOffset() { return offset; }
  public void setOffset(long offset) { this.offset = offset; }

  public long getKafkaTimestamp() { return kafkaTimestamp; }
  public void setKafkaTimestamp(long kafkaTimestamp) { this.kafkaTimestamp = kafkaTimestamp; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RawRecord rawRecord = (RawRecord) o;
    return partition == rawRecord.partition
        && offset == rawRecord.offset
        && kafkaTimestamp == rawRecord.kafkaTimestamp
        && Objects.equals(payload, rawRecord.payload)
        && Objects.equals(topic, rawRecord.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(payload, topic, partition, offset, kafkaTimestamp);
  }

  @Override
  public String toString() {
    return "RawRecord{"
        + "topic='" + topic + '\''
        + ", partition=" + partition
        + ", offset=" + offset
        + ", kafkaTimestamp=" + kafkaTimestamp
        + ", payload='" + payload + '\''
        + '}';
  }
}
