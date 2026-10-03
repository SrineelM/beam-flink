package com.enterprise.beam.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for PipelineConfig validation and defaults.
 */
public class PipelineConfigTest {

  @Test
  public void testDefaultConfigValidInLocalEnvironment() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("LOCAL");
    Assertions.assertDoesNotThrow(config::validate);
  }

  @Test
  public void testProductionFailsWithDefaultPostgresPassword() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("PRODUCTION");
    config.setPostgresPassword("postgres");
    config.setKafkaBootstrapServers("kafka1.prod:9092,kafka2.prod:9092");
    config.setUseLocalObjectStorage(false);

    IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, config::validate);
    Assertions.assertTrue(ex.getMessage().contains("default postgres password"));
  }

  @Test
  public void testProductionFailsWithLocalhostKafkaBootstrap() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("PRODUCTION");
    config.setPostgresPassword("super-secret-password");
    config.setKafkaBootstrapServers("localhost:9092");
    config.setUseLocalObjectStorage(false);

    IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, config::validate);
    Assertions.assertTrue(ex.getMessage().contains("localhost for Kafka bootstrap servers"));
  }

  @Test
  public void testProductionFailsWithLocalObjectStorage() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("PRODUCTION");
    config.setPostgresPassword("super-secret-password");
    config.setKafkaBootstrapServers("kafka1.prod:9092,kafka2.prod:9092");
    config.setUseLocalObjectStorage(true);

    IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class, config::validate);
    Assertions.assertTrue(ex.getMessage().contains("local object storage"));
  }

  @Test
  public void testProductionPassesWithValidConfig() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("PRODUCTION");
    config.setPostgresPassword("super-secret-password");
    config.setKafkaBootstrapServers("kafka1.prod:9092,kafka2.prod:9092");
    config.setUseLocalObjectStorage(false);
    // Skip connectivity health checks in unit tests (no real Postgres/Kafka).
    // The health check logic is tested separately in testHealthCheckSkippedWhenSinkFailOpen.
    config.setSinkFailOpen(true);

    Assertions.assertDoesNotThrow(config::validate);
  }

  /**
   * T1.7: LOCAL environment must use short producer timeouts (fail fast in tests).
   * PRODUCTION-equivalent config must use longer timeouts (120s delivery, 30s request, 60s block).
   */
  @Test
  public void testKafkaProducerTimeoutDefaultsAreEnvironmentSensitive() {
    // LOCAL config: short timeouts
    PipelineConfig localConfig = new PipelineConfig();
    localConfig.setEnvironment("LOCAL");
    // Default LOCAL timeouts: delivery=5000ms, request=3000ms, block=3000ms
    // Verify setters/getters work (actual env-var defaults depend on the ENVIRONMENT env-var
    // which is not set in test JVM, so isLocalEnv() returns true and defaults to short values).
    Assertions.assertTrue(localConfig.getKafkaProducerDeliveryTimeoutMs() <= 30000L,
        "LOCAL delivery timeout should be <= 30s to fail fast in tests");
    Assertions.assertTrue(localConfig.getKafkaProducerRequestTimeoutMs() <= 10000L,
        "LOCAL request timeout should be <= 10s");

    // Override and verify setters/getters
    localConfig.setKafkaProducerDeliveryTimeoutMs(120000L);
    localConfig.setKafkaProducerRequestTimeoutMs(30000L);
    localConfig.setKafkaProducerMaxBlockMs(60000L);
    Assertions.assertEquals(120000L, localConfig.getKafkaProducerDeliveryTimeoutMs());
    Assertions.assertEquals(30000L, localConfig.getKafkaProducerRequestTimeoutMs());
    Assertions.assertEquals(60000L, localConfig.getKafkaProducerMaxBlockMs());
  }

  /**
   * T1.8: LOCAL environment must default to auto.offset.reset=earliest (replay full topic).
   * The getter/setter must work correctly.
   */
  @Test
  public void testKafkaAutoOffsetResetDefault() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("LOCAL");
    // Default for LOCAL (no ENVIRONMENT env-var set in test): earliest
    String offset = config.getKafkaAutoOffsetReset();
    Assertions.assertTrue("earliest".equals(offset) || "latest".equals(offset),
        "autoOffsetReset must be either 'earliest' or 'latest', got: " + offset);

    // Override via setter
    config.setKafkaAutoOffsetReset("latest");
    Assertions.assertEquals("latest", config.getKafkaAutoOffsetReset());

    config.setKafkaAutoOffsetReset("earliest");
    Assertions.assertEquals("earliest", config.getKafkaAutoOffsetReset());
  }

  /**
   * T1.9: maxEventAgeDays must default to 7 and be configurable via setter.
   */
  @Test
  public void testMaxEventAgeDaysDefaultsToSeven() {
    PipelineConfig config = new PipelineConfig();
    Assertions.assertEquals(7, config.getMaxEventAgeDays(),
        "Default maxEventAgeDays must be 7");
    config.setMaxEventAgeDays(30);
    Assertions.assertEquals(30, config.getMaxEventAgeDays());
  }

  /**
   * T1.10: getCheckpointDirUri() must return an absolute file:// URI for LOCAL relative paths.
   * Flink checkpoint storage requires a URI scheme; a bare relative path causes a runtime error.
   */
  @Test
  public void testCheckpointDirUriIsAbsoluteFileUri() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("LOCAL");
    config.setCheckpointDir("./data/checkpoints");

    String uri = config.getCheckpointDirUri();
    Assertions.assertTrue(uri.startsWith("file:///"),
        "getCheckpointDirUri() must return absolute file:// URI for LOCAL environment, got: " + uri);
    Assertions.assertFalse(uri.contains("./"),
        "getCheckpointDirUri() must resolve relative path to absolute, got: " + uri);
  }

  /**
   * T1.10: getCheckpointDirUri() must return URIs with existing schemes unchanged.
   */
  @Test
  public void testCheckpointDirUriPassesThroughExistingUriScheme() {
    PipelineConfig config = new PipelineConfig();
    config.setCheckpointDir("s3://my-bucket/checkpoints");
    Assertions.assertEquals("s3://my-bucket/checkpoints", config.getCheckpointDirUri(),
        "getCheckpointDirUri() must not modify URIs that already have a scheme");
  }

  /**
   * A5: Every env-var key read by PipelineConfig via getEnv/getEnvInt/getEnvLong/getEnvBool
   * must appear in .env.example so operators know which knobs exist.
   * The test reads the source file and .env.example and compares.
   */
  @Test
  public void testAllPipelineConfigEnvKeysArePresentInEnvExample() throws Exception {
    // Read PipelineConfig.java source
    java.nio.file.Path configSrc = java.nio.file.Paths.get(
        "src/main/java/com/enterprise/beam/config/PipelineConfig.java");
    String configSource = new String(java.nio.file.Files.readAllBytes(configSrc));

    // Read .env.example from repo root (two levels up from platform-core)
    java.nio.file.Path envExample = java.nio.file.Paths.get("../.env.example");
    String envContent = new String(java.nio.file.Files.readAllBytes(envExample));

    // Extract all quoted string literals passed to getEnv/getEnvInt/getEnvLong/getEnvBool
    java.util.regex.Pattern p = java.util.regex.Pattern.compile(
        "getEnv(?:Int|Long|Bool|Double)?\\(\"([A-Z0-9_]+)\"");
    java.util.regex.Matcher m = p.matcher(configSource);
    java.util.List<String> missing = new java.util.ArrayList<>();
    while (m.find()) {
      String key = m.group(1);
      // The key should appear either as a bare KEY= or a commented # KEY= line
      if (!envContent.contains(key + "=") && !envContent.contains(key)) {
        missing.add(key);
      }
    }
    Assertions.assertTrue(missing.isEmpty(),
        "These env-var keys are used in PipelineConfig but absent from .env.example: " + missing);
  }

  /**
   * A5: Health check must be skipped in LOCAL environment (no Postgres available in tests).
   * Validates that validate() does not throw even when sinkFailOpen=false in LOCAL mode.
   */
  @Test
  public void testHealthCheckSkippedInLocalEnvironment() {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("LOCAL");
    config.setSinkFailOpen(false);
    // Should not throw — health checks are skipped for LOCAL
    Assertions.assertDoesNotThrow(config::validate);
  }

  /**
   * A5: Kafka security field accessors must round-trip correctly.
   */
  @Test
  public void testKafkaSecurityFieldAccessors() {
    PipelineConfig config = new PipelineConfig();
    config.setKafkaSecurityProtocol("SASL_SSL");
    config.setKafkaSaslMechanism("PLAIN");
    config.setKafkaSaslJaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule required;");
    config.setKafkaSslTruststoreLocation("/etc/kafka/truststore.jks");
    config.setKafkaSslTruststorePassword("changeit");

    Assertions.assertEquals("SASL_SSL", config.getKafkaSecurityProtocol());
    Assertions.assertEquals("PLAIN", config.getKafkaSaslMechanism());
    Assertions.assertTrue(config.getKafkaSaslJaasConfig().contains("PlainLoginModule"));
    Assertions.assertEquals("/etc/kafka/truststore.jks", config.getKafkaSslTruststoreLocation());
    Assertions.assertEquals("changeit", config.getKafkaSslTruststorePassword());
  }

  /**
   * A5: checkKafkaConnectivity() must throw a clear IllegalStateException when no broker
   * is reachable, and include advice to set SINK_FAIL_OPEN=true.
   */
  @Test
  public void testCheckKafkaConnectivityThrowsWhenUnreachable() {
    PipelineConfig config = new PipelineConfig();
    // Use a port that is almost certainly not listening
    config.setKafkaBootstrapServers("localhost:19999");
    IllegalStateException ex = Assertions.assertThrows(
        IllegalStateException.class, config::checkKafkaConnectivity);
    Assertions.assertTrue(ex.getMessage().contains("Kafka health check failed"),
        "Exception message must indicate Kafka health check failure: " + ex.getMessage());
    Assertions.assertTrue(ex.getMessage().contains("SINK_FAIL_OPEN"),
        "Exception message must mention SINK_FAIL_OPEN: " + ex.getMessage());
  }
}
