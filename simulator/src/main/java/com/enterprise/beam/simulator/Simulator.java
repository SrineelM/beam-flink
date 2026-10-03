package com.enterprise.beam.simulator;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.util.StructuredLogger;
import picocli.CommandLine;

import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Main application entry for synthetic event simulator.
 */
@CommandLine.Command(name = "simulator", mixinStandardHelpOptions = true, version = "1.0")
public class Simulator implements Callable<Integer> {

  private static final StructuredLogger LOG = StructuredLogger.getLogger(Simulator.class, "Simulator");

  @CommandLine.Mixin
  private final SimulatorConfig config = new SimulatorConfig();

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Simulator()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    LOG.info("Starting Event Simulator with target rate: " + config.getEventsPerSecond() + " eps", null, null);

    OrderStateMachine stateMachine = new OrderStateMachine();
    EventFactory factory = new EventFactory(config);
    Random random = new Random(config.getSeed());

    try (KafkaProducerWrapper producer = new KafkaProducerWrapper(config.getBootstrapServers())) {
      long startTime = System.currentTimeMillis();
      long eventsProduced = 0;

      while (config.getDurationSeconds() == 0 || (System.currentTimeMillis() - startTime) < config.getDurationSeconds() * 1000L) {
        
        // Malformed scenario
        if (random.nextDouble() < config.getMalformedRate()) {
          producer.sendRaw("orders", "malformed-key", factory.generateMalformedJson());
        } else {
          BaseEvent event = factory.createEvent(stateMachine);
          String topic = event.getEventType().getDefaultTopic();
          producer.send(topic, event);

          // Duplicate event scenario
          if (random.nextDouble() < config.getDuplicateRate()) {
            producer.send(topic, event);
          }
        }

        eventsProduced++;

        // Simple rate limiting pause
        if (config.getEventsPerSecond() > 0 && eventsProduced % config.getEventsPerSecond() == 0) {
          Thread.sleep(1000);
        }
      }
    }

    LOG.info("Simulation completed successfully.", null, null);
    return 0;
  }
}
