create table T_SUPPL_LOG_SYNC
(
	RS_ID VARCHAR2(32 BYTE) , 
	SSN NUMBER(19,0), 
	SCN NUMBER(19,0), 
	REMARK VARCHAR2(5000 BYTE),
	INSERT_TIME NUMBER(19,0),
	primary key (RS_ID, SSN)
) WITH "template=REPLICATED,backups=0,CACHE_NAME=SUPPL_LOG_SYNC, ATOMICITY=ATOMIC";