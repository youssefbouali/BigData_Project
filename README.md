
# NetFlow Big Data Project (Dockerized scaffold)
This scaffold provides a starting point to deploy a pipeline for NetFlow/PCAP ingestion and analysis:
- Zookeeper + Kafka
- Cassandra
- Elasticsearch + Kibana
- Pcap processor (tcpdump + nfdump + softflowd) -- builds into a container that can read PCAPs and export CSV
- CSV -> Kafka producer (Python)
- Spark job runner (executes a PySpark Structured Streaming job that reads Kafka and writes to Cassandra & Elasticsearch)

**Important notes before running**
- This scaffold is an opinionated starting template. Some connectors (Spark Cassandra / Elasticsearch) require JARs compatible with your Spark version. You may need to supply or tune the spark-submit command in `spark_job/Dockerfile` and `/spark_job/run_spark.sh`.
- For CICFlowMeter (Java jar), place `CICFlowMeter.jar` inside `cicflowmeter/` before running the pcap_processor service.
- The pcap_processor runs with `privileged: true` for packet capture tools; on some hosts you may need additional capabilities or run docker-compose with sudo.

**How to run**
1. Place any PCAPs you want to process into `pcap/` directory.
2. (Optional) Put `CICFlowMeter.jar` into `cicflowmeter/` if you want richer features.
3. Start services:
   ```bash
   docker-compose up --build
   ```
4. Watch logs for `pcap_processor` to see CSV files emitted under `output/` then `producer` will push them to Kafka; `spark_job` will process.
