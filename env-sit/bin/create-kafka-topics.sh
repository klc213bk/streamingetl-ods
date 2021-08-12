#!/bin/bash

KAFKA_HOME=/home/kafka/kafka_2.13-2.7.0

cd ${KAFKA_HOME}

./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_COMMISION_FEE
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_CONTRACT_EXTEND_CX
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_CONTRACT_EXTEND_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_CONTRACT_PRODUCT_LOG
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_IMAGE
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.JBPM_VARIABLEINSTANCE
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_POLICY_CHANGE
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_POLICY_PRINT_JOB
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_PRODUCT_COMMISION
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic EBAOUAT1.LS_EBAO.T_PRODUCTION_DETAIL
