#!/bin/bash

KAFKA_HOME=/home/steven/kafka_2.13-2.7.0
APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

${KAFKA_HOME}/bin/connect-standalone.sh ${APP_HOME}/env-dev/config/logminer_connect-standalone.properties ${APP_HOME}/env-dev/config/OracleSourceConnector.properties
