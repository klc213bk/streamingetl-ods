#!/bin/bash

APP_HOME=/home/kafka/streamingetl-ods

#arg1: yyyy-mm-dd
java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.load.LoadDataApp T_CONTRACT_PRODUCT_LOG $1
