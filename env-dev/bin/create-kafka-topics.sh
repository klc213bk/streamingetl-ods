#!/bin/bash

KAFKA_HOME=/home/steven/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ods.cdc.test_t_contract_product_log.0
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ods.cdc.test_t_production_detail.0
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ods.cdc.t_streaming_etl_health_cdc.0