#!/bin/bash

APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

#arg1: yyyy-mm-dd
java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.load.LoadDataApp JBPM_VARIABLEINSTANCE $1
