package com.enterprise.beam.simulator;

import picocli.CommandLine.Option;

/**
 * Command line options for configuring the event simulator.
 */
public class SimulatorConfig {

  @Option(names = {"-e", "--events-per-second"}, description = "Target events per second (default: 100 on laptop, 50k in prod)")
  private int eventsPerSecond = 100;

  @Option(names = {"-d", "--duration-seconds"}, description = "Duration in seconds (0 for infinite)")
  private int durationSeconds = 0;

  @Option(names = {"-c", "--customers"}, description = "Number of synthetic customer IDs")
  private int customerCount = 1000;

  @Option(names = {"-p", "--products"}, description = "Number of synthetic product IDs")
  private int productCount = 500;

  @Option(names = {"-r", "--regions"}, description = "Comma-separated list of regions")
  private String regions = "IN-SOUTH,IN-NORTH,US-EAST,EU-WEST";

  @Option(names = {"--late-event-rate"}, description = "Fraction of events that arrive late (0.0 - 1.0)")
  private double lateEventRate = 0.05;

  @Option(names = {"--max-late-seconds"}, description = "Maximum delay in seconds for late events")
  private int maxLateSeconds = 120;

  @Option(names = {"--duplicate-rate"}, description = "Fraction of duplicate events emitted (0.0 - 1.0)")
  private double duplicateRate = 0.02;

  @Option(names = {"--out-of-order-rate"}, description = "Fraction of out-of-order events (0.0 - 1.0)")
  private double outOfOrderRate = 0.10;

  @Option(names = {"--malformed-rate"}, description = "Fraction of malformed JSON events (0.0 - 1.0)")
  private double malformedRate = 0.01;

  @Option(names = {"--payment-failure-rate"}, description = "Fraction of payments that fail")
  private double paymentFailureRate = 0.15;

  @Option(names = {"--cancellation-rate"}, description = "Fraction of orders cancelled")
  private double cancellationRate = 0.05;

  @Option(names = {"--seed"}, description = "Random seed for reproducible tests")
  private long seed = 42L;

  @Option(names = {"-b", "--bootstrap-servers"}, description = "Kafka bootstrap servers")
  private String bootstrapServers = "localhost:9092";

  public int getEventsPerSecond() { return eventsPerSecond; }
  public int getDurationSeconds() { return durationSeconds; }
  public int getCustomerCount() { return customerCount; }
  public int getProductCount() { return productCount; }
  public String[] getRegionList() { return regions.split(","); }
  public double getLateEventRate() { return lateEventRate; }
  public int getMaxLateSeconds() { return maxLateSeconds; }
  public double getDuplicateRate() { return duplicateRate; }
  public double getOutOfOrderRate() { return outOfOrderRate; }
  public double getMalformedRate() { return malformedRate; }
  public double getPaymentFailureRate() { return paymentFailureRate; }
  public double getCancellationRate() { return cancellationRate; }
  public long getSeed() { return seed; }
  public String getBootstrapServers() { return bootstrapServers; }
}
