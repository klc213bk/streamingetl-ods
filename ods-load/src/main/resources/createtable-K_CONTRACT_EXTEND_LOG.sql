CREATE TABLE K_CONTRACT_EXTEND_LOG
(
"CHANGE_ID" NUMBER(19,0) NOT NULL, 
	"LOG_TYPE" CHAR(1 BYTE) NOT NULL, 
	"POLICY_CHG_ID" NUMBER(19,0), 
	"ITEM_ID" NUMBER(19,0) NOT NULL, 
	"DUE_DATE" DATE NOT NULL, 
	"POLICY_YEAR" NUMBER(3,0) NOT NULL, 
	"POLICY_PERIOD" NUMBER(4,0) NOT NULL, 
	"STRGY_DUE_DATE" DATE DEFAULT null NOT NULL, 
	"PREM_STATUS" NUMBER(2,0) DEFAULT null NOT NULL, 
	"LOG_ID" NUMBER(19,0) NOT NULL, 
	"SA_DUE_DATE" DATE, 
	"LAST_CMT_FLG" CHAR(1 BYTE) DEFAULT 'N' NOT NULL, 
	"EMS_VERSION" NUMBER(10,0) DEFAULT 0 NOT NULL, 
	"INSERTED_BY" NUMBER(19,0) NOT NULL, 
	"UPDATED_BY" NUMBER(19,0) NOT NULL, 
	"INSERT_TIME" DATE NOT NULL, 
	"UPDATE_TIME" DATE NOT NULL, 
	"INSERT_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"UPDATE_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"POLICY_ID" NUMBER(19,0) NOT NULL, 
	"BILLING_DATE" DATE, 
	"REMINDER_DATE" DATE, 
	"INDX_DUE_DATE" DATE, 
	"INDX_REJECT" NUMBER(2,0) DEFAULT 0 NOT NULL, 
	"INSURABILITY_DUE_DATE" DATE, 
	"INSURABILITY_REJECT_COUNT" NUMBER(4,0) DEFAULT 0 NOT NULL, 
	"INSURABILITY_REJECT_REASON" VARCHAR2(1000 BYTE), 
	"BILL_TO_DATE" DATE, 
	"BUCKET_FILLING_DUE_DATE" DATE, 
	"ILP_DUE_DATE" DATE, 
	"WAIVER_SOURCE" NUMBER(2,0), 
	 CONSTRAINT "PK_K_CONTRACT_EXTEND_LOG" PRIMARY KEY ("LOG_ID")
)