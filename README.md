###### streamingetl-ods
 
1. create topics(Optional, first time)
	$./create-kafka-topics.sh
	
2. start ignite server
  	$./start-ignite.sh
  	
3. Initial load data (Optional only for reload data)
	$./start-initload.sh  
	
4.	$./start-load-PolicyPrintJob.sh	
	
5. start kafka consumer
 	$ ./start-consumer.sh
 	
6. start logminer
	$ ./start-logminer.sh 