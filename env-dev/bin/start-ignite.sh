#!/bin/bash

IGNITE_HOME=/home/steven/apache-ignite-2.9.1-bin
APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

${IGNITE_HOME}/bin/ignite.sh ${APP_HOME}/env-dev/config/ignite-config.xml

