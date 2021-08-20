###### streamingetl-ods
 
1. create topics(Optional, first time)
	$./create-kafka-topics.sh

2. Initial load data (Optional only for reload data)
	$./start-initload.sh  
	
3.	$./start-load-PolicyPrintJob.sh	
	
4. start kafka consumer
 	$ ./start-consumer.sh
 	
5. start logminer
	$ ./start-logminer.sh 