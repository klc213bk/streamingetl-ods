#!/bin/bash

APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

#arg1: yyyy-mm-dd, #arg1: yyyy-mm-dd
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_COMMISION_FEE $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_CONTRACT_EXTEND_CX $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_CONTRACT_EXTEND_LOG $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_CONTRACT_PRODUCT_LOG $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_IMAGE $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp JBPM_VARIABLEINSTANCE $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_POLICY_CHANGE $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_POLICY_PRINT_JOB $1 $2
#java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_PRODUCT_COMMISION $1 $2
java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.update.UpdateDataApp T_PRODUCTION_DETAIL $1 $2
