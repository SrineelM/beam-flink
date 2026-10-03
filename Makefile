# Enterprise Apache Beam + Flink Data Platform Makefile

.PHONY: help setup compile test clean run simulator \
        run-direct run-flink-local e2e-local integration-test

help:
	@echo "Available commands:"
	@echo "  make setup             - Download local Kafka and Flink binaries"
	@echo "  make compile           - Compile Maven multi-module project"
	@echo "  make test              - Run unit tests (DirectRunner)"
	@echo "  make integration-test  - Run IT tests (FlinkRunner [local] mini-cluster)"
	@echo "  make clean             - Clean Maven build targets"
	@echo "  make run-direct        - Run pipeline with DirectRunner + sample source"
	@echo "  make run-flink-local   - Run pipeline via mvn exec:java on local Flink cluster"
	@echo "  make e2e-local         - Full local E2E: Kafka + Postgres + Flink (see B1)"
	@echo "  make simulator         - Run the event simulator (requires Kafka)"

compile:
	mvn clean compile

test:
	mvn test

integration-test:
	mvn verify -Pintegration-test -pl pipeline -am

clean:
	mvn clean

# A1/B1: Run pipeline with DirectRunner and sample source (no external services needed).
# Exits after the bounded sample runs through all stages.
run-direct:
	mvn -pl pipeline -am compile exec:java \
	  -Dexec.mainClass=com.enterprise.beam.pipeline.StreamingPipeline \
	  -Dexec.classpathScope=compile \
	  -Dexec.args="--runner=DirectRunner --useSampleSource=true" \
	  -DSINK_FAIL_OPEN=true

# A1: Run pipeline on the local Flink cluster using mvn exec:java.
# Flink deps are scope=provided so -Dexec.classpathScope=compile is required.
# Prerequisite: start the local Flink cluster first with: make setup && ./scripts/local/start-flink.sh
# Then submit via flink run (preferred over exec:java for production-like test):
#   flink run -p 2 pipeline/target/pipeline-1.0.0-SNAPSHOT.jar \
#     --runner=FlinkRunner --flinkMaster=[local] --streaming=true
run-flink-local:
	@echo "Building shaded JAR..."
	mvn -pl pipeline -am package -DskipTests -q
	@echo "Submitting to local Flink cluster at localhost:8081 ..."
	@echo "If 'flink' is not on PATH, set FLINK_HOME and run: \$$FLINK_HOME/bin/flink run ..."
	flink run -p 2 \
	  pipeline/target/pipeline-1.0.0-SNAPSHOT.jar \
	  --runner=FlinkRunner \
	  --flinkMaster=localhost:8081 \
	  --streaming=true \
	  --useSampleSource=false
	@echo ""
	@echo "Check job status at: http://localhost:8081"

# B1: Full local E2E test using real Kafka, Postgres, and local Flink cluster.
# See scripts/local/e2e-local.sh for details and prerequisites.
e2e-local:
	bash scripts/local/e2e-local.sh

setup:
	chmod +x scripts/local/*.sh
	./scripts/local/install-kafka.sh
	./scripts/local/install-flink.sh

simulator:
	mvn -pl simulator -am exec:java \
	  -Dexec.mainClass=com.enterprise.beam.simulator.EventSimulator \
	  -Dexec.classpathScope=compile
