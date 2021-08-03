CREATE TABLE K_IMAGE
(	
"IMAGE_ID" NUMBER(19,0) NOT NULL, 
	"POLICY_ID" NUMBER(19,0), 
	"IMAGE_TYPE_ID" NUMBER(19,0) NOT NULL, 
	"SEQ_NUMBER" NUMBER(5,0), 
	"IMAGE_FORMAT" VARCHAR2(20 BYTE) NOT NULL, 
	"IMAGE_DATA" BLOB, 
	"SCAN_TIME" DATE NOT NULL, 
	"EMP_ID" NUMBER(19,0) NOT NULL, 
	"HEAD_ID" NUMBER(19,0), 
	"FILE_CODE" VARCHAR2(20 BYTE) NOT NULL, 
	"PROCESS_STATUS" CHAR(1 BYTE) DEFAULT 0 NOT NULL, 
	"GROUP_POLICY_ID" NUMBER(19,0), 
	"CASE_ID" NUMBER(19,0), 
	"CHANGE_ID" NUMBER(19,0), 
	"IMAGE_LOCATION" NUMBER(2,0) DEFAULT 1 NOT NULL, 
	"IMAGE_FILE_NAME" VARCHAR2(255 BYTE), 
	"AUTH_CODE" VARCHAR2(20 BYTE), 
	"ORGAN_ID" VARCHAR2(40 BYTE) NOT NULL, 
	"SUB_FILE_CODE" VARCHAR2(20 BYTE), 
	"BUSINESS_ORGAN" VARCHAR2(40 BYTE), 
	"IS_PRIORITY" CHAR(1 BYTE) DEFAULT 'N' NOT NULL, 
	"ZIP_DATE" DATE, 
	"IS_CLEAR" CHAR(1 BYTE) DEFAULT 'Y' NOT NULL, 
	"LIST_ID" NUMBER(19,0), 
	"INSERT_TIME" DATE NOT NULL, 
	"INSERTED_BY" NUMBER(19,0) NOT NULL, 
	"UPDATE_TIME" DATE NOT NULL, 
	"UPDATED_BY" NUMBER(19,0) NOT NULL, 
	"INSERT_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"UPDATE_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"DEPT_ID" NUMBER(19,0), 
	"COMPANY_CODE" VARCHAR2(10 BYTE), 
	"PERSONAL_CODE" VARCHAR2(6 BYTE), 
	"BATCH_DEPT_TYPE" VARCHAR2(2 BYTE), 
	"BATCH_DATE" VARCHAR2(7 BYTE), 
	"BATCH_AREA" VARCHAR2(6 BYTE), 
	"BATCH_DOC_TYPE" VARCHAR2(2 BYTE), 
	"BOX_NO" VARCHAR2(30 BYTE), 
	"REMARK" VARCHAR2(300 BYTE), 
	"SIGNATURE" CHAR(1 BYTE) DEFAULT 'N' NOT NULL, 
	"REAL_WIDTH" NUMBER(5,0) DEFAULT 2496 NOT NULL, 
	"SIG_SEQ_NUMBER" NUMBER(3,0), 
	"SCAN_ORDER" NUMBER(4,0), 
	 CONSTRAINT "PK_K_IMAGE" PRIMARY KEY ("IMAGE_ID")
);   