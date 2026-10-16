# Awesome-Stream-Processing

# Top Stream Processing Tools Ecosystem

**Curated List of SaaS Products & Open-Source GitHub Projects**  
*Focused on Real-Time Event Processing, Durable State & Continuous SQL Analytics*  
**Last updated: March 2026**

This repository tracks notable **SaaS platforms** and **open-source projects** for **Stream Processing**. These tools ingest unbounded event streams from Kafka, Kinesis, Pulsar, CDC sources, and IoT devices, then continuously transform, aggregate, join, and route data — enabling real-time analytics, ETL, anomaly detection, and event-driven applications.

**Examples** include AWS Kinesis Data Analytics, Google Cloud Dataflow, Azure Stream Analytics, Confluent Cloud, Apache Flink, Databricks Structured Streaming, Redpanda Cloud, StreamNative Cloud, Decodable, and Hazelcast (the category leaders).

**Open-source emphasis**: This section is heavily expanded with every major active project for self-hosting, SQL-native stream processing, and transparent state management — ideal for data engineers prioritizing data sovereignty, developers building real-time pipelines, and organizations avoiding per-CSU or per-event cloud pricing.

Contributions welcome! Open a PR to add/update entries. Keep descriptions factual and link to official sites.

## Table of Contents
- [SaaS/Hosted Platforms](#saas-hosted-platforms)
- [Open-Source GitHub Projects](#open-source-github-projects)
- [How to Contribute](#how-to-contribute)
- [Disclaimer](#disclaimer)

## SaaS/Hosted Platforms

- **[Confluent Cloud (ksqlDB)](https://www.confluent.io/product/ksqldb/)**  
  **Managed stream processing database purpose-built for Kafka.** ksqlDB provides an intuitive SQL-like syntax to query, read, write, and process data in Kafka in real-time at scale, without requiring Java or Scala proficiency [citation:1]. Built on Kafka Streams under the hood — "use ksqlDB when you can" is a fair rule of thumb, reserving Kafka Streams for complex stateful logic beyond SQL's reach [citation:15]. Features **push queries** (continuous subscription to changing results) and **pull queries** (point-in-time lookup), removing the need for separate serving systems [citation:8]. **Embedded connectors** run pre-built Kafka Connect connectors directly on ksqlDB servers, eliminating a separate Connect cluster [citation:8]. Supports exactly-once processing semantics and integrates with Schema Registry for Avro [citation:1]. **Confluent Community License** — source-available but not OSI-approved open source [citation:15]. Confluent Cloud pricing is consumption-based per CSU (Confluent Streaming Unit) at roughly **$0.12/hour** [citation:10]. **Limitations**: Only works with Kafka topics as source and sink; SQL dialect is non-standard and has limitations [citation:10].

- **[AWS Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/)**  
  Fully managed service for processing streaming data with Apache Flink, SQL, or Java. Integrates with Kinesis Data Streams, Firehose, and Lambda.

- **[Google Cloud Dataflow](https://cloud.google.com/dataflow)**  
  Fully managed Apache Beam runner for batch and streaming with autoscaling and exactly-once semantics.

- **[Azure Stream Analytics](https://azure.microsoft.com/en-us/products/stream-analytics/)**  
  Real-time analytics service with SQL-based queries over streaming data from IoT Hub, Event Hubs, and Blob Storage.

- **[Databricks Structured Streaming](https://www.databricks.com/)**  
  Apache Spark-based stream processing with Delta Lake integration, exactly-once guarantees, and unified batch/streaming APIs.

- **[Redpanda Cloud](https://redpanda.com/)**  
  Managed Kafka API-compatible streaming platform with built-in tiered storage. Redpanda itself is **source-available under BSL license**, not OSI open source [citation:12].

- **[StreamNative Cloud](https://streamnative.io/)**  
  Managed Apache Pulsar service with Kafka protocol compatibility, tiered storage, and stream processing capabilities.

- **[Decodable](https://www.decodable.co/)**  
  Managed stream processing platform built on Apache Flink with SQL-based pipelines and connector ecosystem.

- **[Hazelcast Cloud](https://hazelcast.com/)**  
  Managed real-time data platform combining stream processing with a fast data store. Hazelcast Jet provides distributed stream and batch processing [citation:13].

## Open-Source GitHub Projects

- **[ksqlDB](https://github.com/confluentinc/ksql)**  
  **The event streaming database purpose-built for stream processing on Kafka.** Provides an intuitive SQL interface for constructing streaming ETL pipelines, responding to real-time business requests, and spotting anomalies — without writing Java or Scala [citation:1]. Built on Kafka Streams, exposing a large subset of its functionality through an easier-to-use, easier-to-deploy package [citation:15]. Supports **push and pull queries**, user-defined functions (UDFs) in Java, and embedded connectors [citation:8]. **Exactly-once processing** configurable via `processing.guarantee` setting [citation:1]. Scales horizontally by starting additional ksqlDB servers with the same config during live operations; maximum parallelism depends on partition count [citation:1]. **Confluent Community License** — free to download, modify, and redistribute for most purposes, but not OSI-approved [citation:15].

- **[Apache Flink](https://github.com/apache/flink)**  
  **The leading open-source framework for stateful computations over unbounded and bounded data streams.** Supports event-time processing, exactly-once semantics, and runs on all common cluster environments. Apache 2.0 licensed. The standard for complex stateful stream processing where SQL alone is insufficient.

- **[RisingWave](https://github.com/risingwavelabs/risingwave)**  
  **Open-source streaming database with PostgreSQL wire compatibility and disaggregated S3-backed storage.** Apache 2.0 licensed. Compute nodes run the Rust-based streaming engine, meta nodes manage cluster state, and compactor nodes handle storage compaction — state stored in S3 simplifies scaling and recovery [citation:10]. Sources include Kafka, Pulsar, Kinesis, PostgreSQL CDC, MySQL CDC, and more. **Sub-second latency** for most queries, with S3-backed storage adding some overhead compared to memory-only systems but making large state workloads more cost-effective [citation:10]. **PostgreSQL-compatible SQL** including materialized views, joins, window functions, and UDFs — connects via any PostgreSQL driver [citation:10]. **The open-source alternative to Confluent Cloud** — one system consolidates ingestion, processing, state management, and serving, eliminating the Kafka Connect + ksqlDB + downstream database stack [citation:18]. Self-managed is free; RisingWave Cloud starts at approximately **$0.14/hour** for small configurations [citation:10].

- **[Timeplus Proton](https://github.com/timeplus-io/proton)**  
  **Fast and lightweight stream processing engine powered by ClickHouse libraries.** C++ implementation with SIMD optimization — delivers **90 million EPS, 4ms end-to-end latency, and high-cardinality aggregation with 1 million unique keys** on an M2 Max MacBook Pro [citation:17]. **Single binary <500MB**, no JVM or other dependencies, runs on AWS t2.nano (1 vCPU, 0.5 GiB memory) [citation:17]. Features streaming ETL, tumble/hop/session windows, watermarks, **incremental materialized views**, CDC, and data revision processing [citation:17]. **Stores queryable analytical/row-based materialized views** within Proton itself for analytics dashboards — unlike pure stream processors [citation:17]. Apache Flink or ksqlDB alternative for Kafka, Redpanda, or Pulsar [citation:17].

- **[Materialize](https://github.com/MaterializeInc/materialize)**  
  **Streaming database maintaining SQL views incrementally** — when source data changes, Materialize updates view results without recomputing from scratch [citation:10]. Built on Timely Dataflow and Differential Dataflow (Rust-based research systems). Sources connect to Kafka, PostgreSQL CDC, or webhook inputs. **PostgreSQL-compatible wire protocol** — use psql, standard SQL drivers, and ORMs [citation:10]. Sub-second view maintenance for most workloads; targets single-digit millisecond updates for simple views and low hundreds of milliseconds for complex multi-way joins [citation:10]. **Managed SaaS only** — the open-source version was deprecated. Pricing starts at roughly **$0.35/hour** for the smallest configuration [citation:10].

- **[Apache Kafka Streams](https://github.com/apache/kafka)**  
  **Lightweight, powerful Java library for enriching, transforming, and processing real-time streams of data.** The engine under ksqlDB [citation:15]. Provides DSL (functional programming API) and Processor API (imperative API) — can combine both [citation:1]. Best for full-fledged stream processing applications requiring complex stateful logic like finite state machines, where SQL is insufficient [citation:1]. Included with Apache Kafka distribution.

- **[Apache Beam](https://github.com/apache/beam)**  
  **Unified programming model for batch and streaming data processing** with SDKs for Java, Python, and Go. Pipelines are portable across runners including Flink, Spark, and Google Cloud Dataflow. Apache 2.0 licensed.

- **[Apache Pulsar](https://github.com/apache/pulsar)**  
  **Cloud-native distributed pub-sub messaging and streaming platform** with multi-tenancy, geo-replication, and tiered storage. Includes Pulsar Functions for lightweight stream processing. Apache 2.0 licensed.

- **[Bytewax](https://github.com/bytewax/bytewax)**  
  **Python-first stateful stream processing framework with a Rust-based distributed engine.** Dataflow API with operators like `map`, `filter`, `fold_window`, and `join`. Connectors for Kafka, files, and WebSockets. Apache 2.0 licensed.

- **[Benthos (Redpanda Connect)](https://github.com/redpanda-data/benthos)**  
  **Stream processor with 200+ connectors** for building data pipelines. Declarative configuration with mapping language and support for Kafka, HTTP, and databases. Apache 2.0 licensed.

- **[Feldera](https://github.com/feldera/feldera)**  
  **Incremental computation engine evaluating arbitrary SQL programs incrementally.** Processes millions of events per second on a laptop with full SQL support including joins, aggregates, window functions, and recursive queries.

### Additional Strong Open-Source Options

- **Quix Streams** — Python library for Kafka stream processing with pandas-like API for real-time data transformations [citation:14].
- **Memphis** — Real-time data processing platform with built-in schema management, observability, and dead-letter queues [citation:14].
- **NATS JetStream** — Cloud-native messaging system with persistent streaming, exactly-once delivery, and key-value store. Lightweight alternative to Kafka.
- **Apache Samza** — Distributed stream processing framework built on Kafka with stateful processing and fault tolerance [citation:13].
- **Apache Storm** — Classic distributed real-time computation system. Spouts and bolts model.

**Frameworks for building custom systems**: Combine **ksqlDB** for SQL-native stream processing directly on Kafka topics without writing Java, **RisingWave** for a PostgreSQL-compatible streaming database with S3-backed state and no separate serving layer, **Timeplus Proton** for single-binary lightweight processing with ClickHouse performance, **Materialize** for incremental view maintenance on complex multi-way joins, and **Apache Flink** for arbitrarily complex stateful processing beyond SQL — to build a complete stream processing stack tailored to your latency, throughput, and operational constraints.

## How to Contribute

1. Fork the repo.
2. Add/edit entries in `README.md` (follow existing format).
3. Include: name, link, 1–2 sentence description, and whether it's SaaS or open-source.
4. Submit PR with a short explanation.

Star the repo if you find it useful!

## Disclaimer

- This is a **community-curated** list — not exhaustive and not an endorsement.
- Stream processing tools differ significantly in latency profile, state management approach, and SQL compatibility. **ksqlDB is not OSI-approved open source** — it uses the Confluent Community License, which restricts some commercial uses [citation:15]. **Materialize's open-source version is deprecated** — only the managed SaaS remains [citation:10]. **Redpanda is source-available under BSL, not open source** [citation:12]. **Benchmarks are workload-dependent** — validate against your specific event rate, state size, and query complexity. **SQL compatibility varies** — ksqlDB uses a non-standard dialect; RisingWave and Materialize are PostgreSQL-compatible [citation:1][citation:10].
- Self-hosted open-source solutions require proper Kafka/Pulsar infrastructure management, state backend planning, checkpoint configuration, and monitoring.

---

**Made for data engineers, streaming architects, platform teams, and developers building real-time data pipelines.**  
Let's make stream processing more open, SQL-native, and operationally simple.
