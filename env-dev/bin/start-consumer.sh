#!/bin/bash

APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

java -cp "${APP_HOME}/lib/ods-consumer-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.consumer.ConsumerApp
