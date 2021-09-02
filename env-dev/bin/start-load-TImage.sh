#!/bin/bash

APP_HOME=/home/steven/gitrepo/transglobe/streamingetl-ods

#arg1: yyyy-mm-dd
java -cp "${APP_HOME}/lib/ods-load-1.0.jar:${APP_HOME}/lib/*" -Dprofile.active=env-dev com.transglobe.streamingetl.ods.load.LoadDataApp T_IMAGE $1

STATUS=$?	
if [ ${STATUS} == 0 ]
then
 echo "Status=${STATUS}, load data [ OK ]"
else 
 echo "Status=${STATUS}, load data [ Fail ]"
fi


