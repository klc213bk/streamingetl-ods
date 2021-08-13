#!/bin/bash

LOGMINER_HOME=/home/steven/gitrepo/transglobe/kafka-connect-logminer
COMMON_HOME=/home/steven/gitrepo/transglobe/streamingetl-common
SCHEDULING_HOME=/home/steven/gitrepo/transglobe/streamingetl-scheduling
STREAMINGETL_HOME=/home/steven/gitrepo/transglobe/streamingetl
APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

echo "start to build logminer"
cd ${LOGMINER_HOME}
#mvn clean package

echo "start to build common"
cd ${COMMON_HOME}
#mvn clean install

echo "start to build scheduling"
cd ${SCHEDULING_HOME}
#mvn clean package

echo "start to build streamingetl"
cp ${LOGMINER_HOME}/target/*.jar ${STREAMINGETL_HOME}/connectors/oracle-logminer-connector/
cp ${COMMON_HOME}/target/*.jar "${STREAMINGETL_HOME}/lib"
cp ${SCHEDULING_HOME}/target/*.jar "${STREAMINGETL_HOME}/lib"

echo "start to build app"
cd ${APP_HOME}
mvn clean package
cp ${COMMON_HOME}/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/ods-consumer/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/ods-load/target/*.jar "${APP_HOME}/lib"
cp ${APP_HOME}/ods-test/target/*.jar "${APP_HOME}/lib"